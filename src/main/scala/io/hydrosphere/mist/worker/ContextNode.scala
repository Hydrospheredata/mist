package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Address, Cancellable, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}
import scala.util.{Failure, Random, Success}

class ContextNode(namespace: String) extends Actor with ActorLogging {

  implicit val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.clusterManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress: Address = cluster.selfAddress

  lazy val contextWrapper: NamedContext = NamedContext(namespace)

  private val workerDowntime: Duration = MistConfig.Contexts.downtime(namespace)

  private var cancellableWatchDog: Option[Cancellable] = scheduleDowntime(workerDowntime)

  private val nodeUID = java.util.UUID.randomUUID.toString

  def scheduleDowntime(duration: Duration): Option[Cancellable] = {
    if(duration.isFinite()) {
      Option(context.system.scheduler.scheduleOnce(
        FiniteDuration(duration.toNanos, TimeUnit.NANOSECONDS))(f = {
        serverActor ! RemoveContext(nodeUID)
      })(executionContext))} else { None }
  }

  override def preStart(): Unit = {
    serverActor ! WorkerDidStart(nodeUID, namespace, cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  private val jobDescriptions: ArrayBuffer[JobDetails] = ArrayBuffer.empty[JobDetails]

  type NamedActors = (JobDetails,  () => Unit)
  lazy val namedJobCancellations: ArrayBuffer[(JobDetails, () => Unit)] = ArrayBuffer.empty[NamedActors]

  override def receive: Receive = {

    case jobRequest: JobDetails =>
      if (cancellableWatchDog.nonEmpty) { cancellableWatchDog.get.cancel() }
      log.info(s"[WORKER] received JobDetails: $jobRequest")
      val originalSender = sender

      def cancellable[T](f: Future[T])(cancellationCode: => Unit): (() => Unit, Future[T]) = {
        val p = Promise[T]
        val first = Future firstCompletedOf Seq(p.future, f)
        val cancellation: () => Unit = {
          () =>
            first onFailure { case _ => cancellationCode }
            p failure new Exception
        }
        (cancellation, first)
      }

      try {
        val runner = Runner(jobRequest, contextWrapper)

        val startedJobDetails = jobRequest.starts().withStatus(JobDetails.Status.Running)
        originalSender ! startedJobDetails
        val runnerFuture: Future[JobResponseOrError] = Future {
          log.info(s"${jobRequest.configuration.namespace}#${jobRequest.jobId} is running")

          runner.run()
        }(executionContext)

        val (cancel, cancellableRunnerFuture) = cancellable(runnerFuture) {
          jobDescriptions -= jobRequest
          runner.stop()
          originalSender ! startedJobDetails.ends().withStatus(JobDetails.Status.Aborted).withJobResult(Right("Canceled"))
        }

        jobDescriptions += jobRequest

        namedJobCancellations += ((jobRequest, cancel))

        cancellableRunnerFuture
          .recover {
            case e: Throwable => originalSender ! startedJobDetails.ends().withStatus(JobDetails.Status.Error).withJobResult(Right(e.toString))
          }(ExecutionContext.global)
          .andThen {
            case _ =>
              jobDescriptions -= jobRequest
              if (jobDescriptions.isEmpty) {
                cancellableWatchDog = scheduleDowntime(workerDowntime)
              }
          }(ExecutionContext.global)
          .andThen {
            case Success(result: JobResponseOrError) => originalSender ! startedJobDetails.ends().withStatus(JobDetails.Status.Stopped).withJobResult(result)
            case Failure(error: Throwable) => originalSender ! startedJobDetails.ends().withStatus(JobDetails.Status.Error).withJobResult(Right(error.toString))
          }(ExecutionContext.global)
      } catch {
        case exc: Throwable => originalSender ! jobRequest.ends().withStatus(JobDetails.Status.Stopped).withJobResult(Right(exc.toString))
      }

    case StopWhenAllDo =>
      context.system.scheduler.schedule(FiniteDuration(0, TimeUnit.SECONDS),
        FiniteDuration(1, TimeUnit.MINUTES))(f = {
        if(jobDescriptions.isEmpty) {
          serverActor ! RemoveContext(nodeUID)
        }
      })(executionContext)

    case ListJobs =>
      sender ! jobDescriptions

    case StopJob(jobIdentifier) =>
      val originalSender = sender
      val future: Future[List[String]] = Future {
        val stopResponse = ArrayBuffer.empty[String]
        jobDescriptions.foreach {
          jobDescription: JobDetails => {
            if (jobIdentifier == jobDescription.configuration.externalId.getOrElse("None") || jobIdentifier == jobDescription.jobId) {
              stopResponse += s"Job ${jobDescription.configuration.externalId.getOrElse("")} ${jobDescription.jobId}" +
                " is scheduled for shutdown. It can take a while."
              namedJobCancellations
                .filter(namedJobCancellation => namedJobCancellation._1.jobId == jobDescription.jobId)
                .foreach(namedJobCancellation => namedJobCancellation._2())
            }
          }
        }
        stopResponse.toList
      }

      future onComplete {
        case Success(result: List[String]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        //noinspection ScalaDeprecation
        cluster.system.shutdown()
      }

    case MemberRemoved(member, _) =>
      if (member.address == cluster.selfAddress) {
        sys.exit(0)
      }
  }
}

object ContextNode {
  def props(namespace: String): Props = Props(classOf[ContextNode], namespace)
}
