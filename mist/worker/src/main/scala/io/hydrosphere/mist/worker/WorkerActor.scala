package io.hydrosphere.mist.worker

import java.io.File
import java.util.concurrent.Executors

import akka.actor._
import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector, SimpleRunnerSelector}
import mist.api.data.JsLikeData
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  jobFuture: Future[Either[Throwable, JsLikeData]]
)

trait JobStarting {
  that: Actor =>
  val runnerSelector: RunnerSelector
  val namedContext: NamedContext
  val artifactDownloader: ArtifactDownloader

  protected final def startJob(
    req: RunJobRequest
  )(implicit ec: ExecutionContext): Future[Either[Throwable, JsLikeData]] = {
    val id = req.id
    val s = sender()
    val jobStart = for {
      file   <- downloadFile(s, req)
      runner =  runnerSelector.selectRunner(file)
      res    =  runJob(s, req, runner)
    } yield res

    jobStart.onComplete(r => {
      val message = r match {
        case Success(Right(value)) => JobSuccess(id, value)
        case Success(Left(err)) => failure(req, err)
        case Failure(e) => failure(req, e)
      }
      self ! message
    })
    jobStart
  }

  private def downloadFile(actor: ActorRef, req: RunJobRequest): Future[File] = {
    actor ! JobFileDownloading(req.id)
    artifactDownloader.downloadArtifact(req.params.filePath)
  }

  private def failure(req: RunJobRequest, ex: Throwable): JobResponse =
    JobFailure(req.id, buildErrorMessage(req.params, ex))


  private def runJob(actor: ActorRef, req: RunJobRequest, runner: JobRunner): Either[Throwable, JsLikeData] = {
    val id = req.id
    actor ! JobStarted(id)
    namedContext.sparkContext.setJobGroup(id, id)
    runner.run(req, namedContext)
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
    val logger = LoggerFactory.getLogger(this.getClass)
    val service = Executors.newFixedThreadPool(maxJobs)
    ExecutionContext.fromExecutorService(
      service,
      e => logger.error("Error from thread pool", e)
    )
  }

  override def preStart(): Unit = {
    context.setReceiveTimeout(idleTimeout)
  }

  override def receive: Receive = {
    case req@RunJobRequest(id, params, _) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        val jobStarted = startJob(req)
        activeJobs += id -> ExecutionUnit(sender(), jobStarted)
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
    artifactDownloader.stop()
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
    val logger = LoggerFactory.getLogger(this.getClass)
    val service = Executors.newSingleThreadExecutor()
    ExecutionContext.fromExecutorService(service, t => logger.error("Error from thread pool", t))
  }

  override def receive: Receive = awaitRequest

  override def preStart(): Unit = {
    context.setReceiveTimeout(1.minute)
  }

  val awaitRequest: Receive = {
    case req: RunJobRequest =>
      val jobStarted = startJob(req)
      context.become(execute(ExecutionUnit(sender(), jobStarted)))

    case ReceiveTimeout =>
      log.info("No job requests for a 1 minute - (cluster problems?)")
      context.stop(self)
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
    artifactDownloader.stop()
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
