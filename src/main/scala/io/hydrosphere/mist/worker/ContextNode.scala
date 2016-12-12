package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.FullJobConfiguration
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class ContextNode(namespace: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(namespace)

  override def preStart(): Unit = {
    serverActor ! WorkerDidStart(namespace, cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  lazy val jobDescriptions = ArrayBuffer.empty[JobDescription]

  lazy val senders = ArrayBuffer.empty[String]

  override def receive: Receive = {

    case jobRequest: FullJobConfiguration =>
      log.info(s"[WORKER] received JobRequest: $jobRequest")
      val originalSender = sender

      senders += jobRequest.externalId.getOrElse("")

      lazy val runner = Runner(jobRequest, contextWrapper)

      val jobDescription = new JobDescription(jobRequest.namespace, jobRequest.externalId.getOrElse(""))

//      def cancellable[T](f: Future[T])(customCode: => Unit): (() => Unit, Future[T]) = {
//        val p = Promise[T]
//        val first = Future firstCompletedOf Seq(p.future, f)
//        val cancellation: () => Unit = {
//          () =>
//            first onFailure { case _ => originalSender ! Right("canceled")}
//            p failure new Exception
//        }
//        (cancellation, first)
//      }

      val future: Future[Either[Map[String, Any], String]] = Future {
        if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
          serverActor ! AddJobToRecovery(runner.id, runner.configuration)
        }
        log.info(s"${jobRequest.namespace}#${runner.id} is running")

        runner.run()
      }(executionContext)

//      val (cancel, future1) = cancellable(future) {
//        jobDescriptions -= jobDescription
//        if (MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
//          serverActor ! RemoveJobFromRecovery(runner.id)
//        }
//      }

        jobDescriptions += jobDescription

//      if (!jobDescriptions.contains(jobDescription)) cancel() else {
//        future1
          future
          .recover {
            case e: Throwable => originalSender ! Right(e.toString)
          }(ExecutionContext.global)
          .andThen {
            case _ => {
              jobDescriptions -= jobDescription
              if (MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
                serverActor ! RemoveJobFromRecovery(runner.id)
              }
            }
          }(ExecutionContext.global)
          .andThen {
            case Success(result: Either[Map[String, Any], String]) => originalSender ! result
            case Failure(error: Throwable) => originalSender ! Right(error.toString)
          }(ExecutionContext.global)
//      }


    case ListMessage(message) =>
      val originalSender = sender
      if(message.contains(Constants.CLI.listJobsMsg)) {
        val cliActor = cluster.system.actorSelection(message.substring(Constants.CLI.listJobsMsg.length))
        jobDescriptions.foreach {
          case jobDescription: JobDescription => {
            cliActor ! new StringMessage(s"${Constants.CLI.jobMsgMarker} namespace: ${jobDescription.namespace} extId: ${jobDescription.externalId}")
          }
        }
        cliActor ! new StringMessage(s"${Constants.CLI.jobMsgMarker} it is a all job descriptions in $nodeAddress")
      }

    case StringMessage(message) =>
      val originalSender = sender
      if(message.contains(Constants.CLI.stopJobMsg)) {
        jobDescriptions.map {
          case jobDescription: JobDescription => {
            if(jobDescription.externalId.eq(message.substring(Constants.CLI.stopJobMsg.length))) {
              originalSender ! new StringMessage(s"${Constants.CLI.jobMsgMarker} do`t worry, sometime it will stop")
              jobDescriptions -= jobDescription
            }
          }
        }
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, prevStatus) =>
      if (member.address == cluster.selfAddress) {
        sys.exit(0)
      }
  }
}

object ContextNode {
  def props(namespace: String): Props = Props(classOf[ContextNode], namespace)
}
