package io.hydrosphere.mist.worker

import java.io.File
import java.util.concurrent.Executors

import akka.actor._
import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector, SimpleRunnerSelector}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  jobFuture: Future[Map[String, Any]]
)

trait JobStarting { that: Actor =>
  val runnerSelector: RunnerSelector
  val namedContext: NamedContext
  val artifactDownloader: ArtifactDownloader

  def startJob(req: RunJobRequest, runner: JobRunner)(implicit ec: ExecutionContext): Future[Map[String, Any]] = {
    val id = req.id
    namedContext.sparkContext.setJobGroup(id, id)
    val future = runner.run(req, namedContext)

    future.onComplete(r => {
      val message = r match {
        case Success(value) => JobSuccess(id, value)
        case Failure(e) => JobFailure(id, buildErrorMessage(req.params, e))
      }
      self ! message
    })

    future
  }

  def downloadFile(id: String, filePath: String)(implicit ec: ExecutionContext): Future[File] = {
    val fileDownload = artifactDownloader.downloadArtifact(filePath)
    sender() ! JobFileDownloading(id)
    fileDownload.onComplete {
      case Success(_) =>
        sender() ! JobFileDownloaded(id)
      case Failure(ex) =>
        sender() ! JobFailure(id, ex.getMessage)
    }
    fileDownload
  }

  protected def buildErrorMessage(params: JobParams, e: Throwable): String = {
    val msg = Option(e.getMessage).getOrElse("")
    val trace = e.getStackTrace.map(e => e.toString).mkString("; ")
    s"Error running job with $params. Type: ${e.getClass.getCanonicalName}, message: $msg, trace $trace"
  }
}

class SharedWorkerActor(
  val runnerSelector: RunnerSelector,
  val namedContext: NamedContext,
  val artifactDownloader: ArtifactDownloader,
  idleTimeout: Duration,
  maxJobs: Int
) extends Actor with JobStarting with ActorLogging {

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  implicit val ec = {
    val service = Executors.newFixedThreadPool(maxJobs)
    ExecutionContext.fromExecutorService(
      service,
      e => log.error(e, "Error from thread pool")
    )
  }

  override def preStart(): Unit = {
    context.setReceiveTimeout(idleTimeout)
  }

  override def receive: Receive = {
    case req@RunJobRequest(id, params) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        for {
          file <- downloadFile(req.id, req.params.filePath)
          runner = runnerSelector.selectRunner(file)
          res <- {
            val jobStarted = startJob(req, runner)
            log.info(s"Starting job: $id")
            activeJobs += id -> ExecutionUnit(sender(), jobStarted)
            jobStarted
          }
        } yield res
      }

    // it does not work for streaming jobs
    case CancelJobRequest(id) =>
      activeJobs.get(id) match {
        case Some(u) =>
          namedContext.sparkContext.cancelJobGroup(id)
          sender() ! JobIsCancelled(id)
        case None =>
          log.warning(s"Can not cancel unknown job $id")
      }

    case x: JobResponse =>
      log.info(s"Job execution done. Result $x")
      activeJobs.get(x.id) match {
        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          log.warning(s"Corrupted worker state, unexpected receiving {}", x)
      }

    case ReceiveTimeout if activeJobs.isEmpty =>
      log.info(s"There is no activity on worker: $namedContext.. Stopping")
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
    namedContext.stop()
  }

}

object SharedWorkerActor {

  def props(
    runnerSelector: RunnerSelector,
    context: NamedContext,
    artifactDownloader: ArtifactDownloader,
    idleTimeout: Duration,
    maxJobs: Int
  ): Props =
    Props(new SharedWorkerActor(runnerSelector, context, artifactDownloader, idleTimeout, maxJobs))

}

class ExclusiveWorkerActor(
  val runnerSelector: RunnerSelector,
  val namedContext: NamedContext,
  val artifactDownloader: ArtifactDownloader
) extends Actor with JobStarting with ActorLogging {

  implicit val ec = {
    val service = Executors.newSingleThreadExecutor()
    ExecutionContext.fromExecutorService(service)
  }

  override def receive: Receive = awaitRequest

  val awaitRequest: Receive = {
    case req@RunJobRequest(id, params) => for {
      file <- downloadFile(id, params.filePath)
      runner = runnerSelector.selectRunner(file)
      res <- {
        val jobStarted = startJob(req, runner)
        sender() ! JobStarted(id)
        val unit = ExecutionUnit(sender(), jobStarted)
        context.become(execute(unit))
        log.info(s"Starting job: $id")
        jobStarted
      }
    } yield res
  }

  def execute(executionUnit: ExecutionUnit): Receive = {
    case CancelJobRequest(id) =>
      namedContext.sparkContext.cancelJobGroup(id)
      sender() ! JobIsCancelled(id)
      context.stop(self)

    case x: JobResponse =>
      log.info(s"Job execution done. Result $x")
      executionUnit.requester forward x
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
    namedContext.stop()
  }

}

object ExclusiveWorkerActor {

  def props(runnerSelector: RunnerSelector, context: NamedContext, artifactDownloader: ArtifactDownloader): Props =
    Props(new ExclusiveWorkerActor(runnerSelector, context, artifactDownloader))
}

object WorkerActor {

  def propsFromInitInfo(name: String, contextName: String, mode: WorkerMode): WorkerInitInfo => (NamedContext, Props) = {

    def mkNamedContext(info: WorkerInitInfo): NamedContext = {
      import info._

      val conf = new SparkConf().setAppName(name).setAll(info.sparkConf)
      val sparkContext = new SparkContext(conf)

      val centralLoggingConf = {
        val hostPort = logService.split(":")
        CentralLoggingConf(hostPort(0), hostPort(1).toInt)
      }

      new NamedContext(
        sparkContext,
        contextName,
        org.apache.spark.streaming.Duration(info.streamingDuration.toMillis),
        Option(centralLoggingConf)
      )
    }

    (info: WorkerInitInfo) => {
      val namedContext = mkNamedContext(info)
      val (h, p) = info.masterHttpConf.split(':') match {
        case Array(host, port) => (host, port.toInt)
      }
      val artifactDownloader = ArtifactDownloader.create(h, p, info.jobsSavePath)
      val runnerSelector = new SimpleRunnerSelector
      val props = mode match {
        case Shared => SharedWorkerActor.props(runnerSelector, namedContext, artifactDownloader, info.downtime, info.maxJobs)
        case Exclusive => ExclusiveWorkerActor.props(runnerSelector, namedContext, artifactDownloader)
      }
      (namedContext, props)
    }
  }

}
