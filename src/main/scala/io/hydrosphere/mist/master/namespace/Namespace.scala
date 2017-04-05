package io.hydrosphere.mist.master.namespace

import java.io.File
import java.util.UUID

import akka.pattern._
import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import akka.util.Timeout
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.{JobDetails, JobExecutionParams, JobResult}
import io.hydrosphere.mist.master.JobManager.StartJob
import io.hydrosphere.mist.master.namespace.RemoteWorker._
import io.hydrosphere.mist.utils.Logger

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Namespace(
  name: String,
  system: ActorSystem
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global

  @volatile
  var jobExecutor: ActorRef = _

  def startJob(execParams: JobExecutionParams): Future[JobResult] = onExecutor { ref =>
    val request = RunJobRequest(
      id = s"$name-${execParams.className}-${UUID.randomUUID().toString}",
      JobParams(
        filePath = execParams.path,
        className = execParams.className,
        arguments = execParams.parameters,
        action = execParams.action
      )
    )

    val promise = Promise[JobResult]
    implicit val timeout = Timeout(30.seconds)
    ref.ask(request)
      .mapTo[ExecutionInfo].flatMap(_.promise.future).onComplete({
        case Success(r) =>
            promise.success(JobResult.success(r, execParams))
        case Failure(e) =>
          promise.success(JobResult.failure(e.getMessage, execParams))
    })

    promise.future
  }

  private def onExecutor[T](f: ActorRef => T): T = {
    if (jobExecutor == null) {
      val settings = WorkerSettings(
        name = name,
        runOptions = MistConfig.Contexts.runOptions(name),
        configFilePath = System.getProperty("config.file"),
        jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString

      )
      //TODO ???
      val sparkHome = System.getenv("SPARK_HOME").ensuring(_.nonEmpty, "SPARK_HOME is not defined!")
      new NewWorkerRunner(sparkHome).start(settings)
      jobExecutor = system.actorOf(NamespaceJobExecutor.props(name, 10), s"namespace-$name")
    }
    f(jobExecutor)
  }
}

case class ExecutionInfo(
  request: RunJobRequest,
  promise: Promise[Map[String, Any]]
)

object ExecutionInfo {

  def apply(req: RunJobRequest): ExecutionInfo =
    ExecutionInfo(req, Promise[Map[String, Any]])

}

class NamespaceJobExecutor(
  name: String,
  maxRunningJobs: Int
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  var queue = mutable.Queue[ExecutionInfo]()
  var running = mutable.HashMap[String, ExecutionInfo]()

  override def receive: Actor.Receive = noWorker

  private def noWorker: Receive = {
    case r: RunJobRequest =>
      val info = ExecutionInfo(r)
      queue += info
      sender() ! info

    case MemberUp(member) if isMyWorker(member) =>
      log.info(s"Worker for is up sending jobs ${queue.size}")
      val worker = toWorkerRef(member)
      sendQueued(worker)
      context become withWorker(worker)
  }

  private def withWorker(w: ActorSelection): Receive = {
    case r: RunJobRequest =>
      val info = ExecutionInfo(r)
      if (running.size < maxRunningJobs) {
        sendJob(w, info)
      } else {
        queue += info
      }
      sender() ! info

    case started: JobStarted =>
      log.info(s"Job has been started ${started.id}")

    case success: JobSuccess =>
      running.get(success.id) match {
        case Some(i) =>
          i.promise.success(success.result)
          running -= success.id
        case None =>
          log.warning(s"WTF? $success")
      }
      log.info(s"Job ${success.id} ended")
      sendQueued(w)

    case failure: JobFailure =>
      running.get(failure.id) match {
        case Some(i) =>
          i.promise.failure(new RuntimeException(failure.error))
          running -= failure.id
        case None =>
          log.warning(s"WTF? $failure")
      }
      sendQueued(w)
      log.info(s"Job ${failure.id} is failed")

    case MemberExited(member) if isMyWorker(member) =>
      context become noWorker

    case UnreachableMember(member) if isMyWorker(member) =>
      log.info(s"Worker is unreachable $member")
      context become noWorker

  }

  private def sendQueued(worker: ActorSelection): Unit = {
    val max = maxRunningJobs - running.size
    for {
      _ <- 1 to max
      if queue.nonEmpty
    } yield {
      val info = queue.dequeue()
      sendJob(worker, info)
    }
  }

  private def sendJob(to: ActorSelection, info: ExecutionInfo): Unit = {
    to ! info.request
    running += info.request.id -> info
  }

  private def toWorkerRef(member: Member): ActorSelection = {
    cluster.system.actorSelection(RootActorPath(member.address) / "user"/ workerName)
  }

  private def isMyWorker(member: Member): Boolean =
    member.hasRole(workerName)

  private def workerName: String = s"worker-$name"

}

object NamespaceJobExecutor {

  def props(name: String, maxRunningJobs: Int): Props =
    Props(classOf[NamespaceJobExecutor], name, maxRunningJobs)
}




