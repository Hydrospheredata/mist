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

class WorkerActor(
  val runnerSelector: RunnerSelector,
  val namedContext: NamedContext,
  val artifactDownloader: ArtifactDownloader,
  maxJobs: Int
) extends Actor with JobStarting with ActorLogging {

  implicit val ec = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val service = Executors.newFixedThreadPool(maxJobs)
    ExecutionContext.fromExecutorService(
      service,
      e => logger.error("Error from thread pool", e)
    )
  }

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  override def receive: Receive = process()

  private def process(): Receive = {
    case req: RunJobRequest => tryRun(req, sender())
    case CancelJobRequest(id) => tryCancel(id, sender())
    case resp: JobResponse => onJobComplete(resp)
    case CompleteAndShutdown if activeJobs.isEmpty => self ! PoisonPill
    case CompleteAndShutdown => context become completeAndShutdown()
  }

  private def completeAndShutdown(): Receive = {
    case CancelJobRequest(id) => tryCancel(id, sender())
    case resp: JobResponse =>
      onJobComplete(resp)
      if (activeJobs.isEmpty) self ! PoisonPill
  }

  private def tryRun(req: RunJobRequest, respond: ActorRef): Unit = {
    if (activeJobs.size == maxJobs) {
      respond ! WorkerIsBusy(req.id)
    } else {
      val jobStarted = startJob(req)
      activeJobs += req.id -> ExecutionUnit(respond, jobStarted)
    }
  }


  private def tryCancel(id: String, respond: ActorRef): Unit = activeJobs.get(id) match {
    case Some(_) =>
      namedContext.sparkContext.cancelJobGroup(id)
      respond ! JobIsCancelled(id)
    case None =>
      log.warning(s"Can not cancel unknown job $id")
  }

  private def onJobComplete(resp: JobResponse): Unit = activeJobs.get(resp.id) match {
    case Some(unit) =>
      log.info(s"Job execution done. Result $resp")
      unit.requester ! resp
      activeJobs -= resp.id

    case None =>
      log.warning(s"Corrupted worker state, unexpected receiving {}", resp)
  }

  override def postStop(): Unit = {
    ec.shutdown()
    artifactDownloader.stop()
  }

}

object WorkerActor {

  def props(
    context: NamedContext,
    artifactDownloader: ArtifactDownloader,
    maxJobs: Int,
    runnerSelector: RunnerSelector
  ): Props =
    Props(new WorkerActor(runnerSelector, context, artifactDownloader, maxJobs))

  def props(
    context: NamedContext,
    artifactDownloader: ArtifactDownloader,
    maxJobs: Int
  ): Props =
    Props(new WorkerActor(new SimpleRunnerSelector, context, artifactDownloader, maxJobs))
}

//object WorkerActor {
//
//  def propsFromInitInfo(name: String, contextName: String, mode: WorkerMode): WorkerInitInfo => (NamedContext, Props) = {
//
//    def mkNamedContext(info: WorkerInitInfo): NamedContext = {
//      import info._
//
//      val conf = new SparkConf().setAppName(name).setAll(info.sparkConf)
//      val sparkContext = new SparkContext(conf)
//
//      val centralLoggingConf = {
//        val hostPort = logService.split(":")
//        CentralLoggingConf(hostPort(0), hostPort(1).toInt)
//      }
//
//      new NamedContext(
//        sparkContext,
//        contextName,
//        org.apache.spark.streaming.Duration(info.streamingDuration.toMillis),
//        Option(centralLoggingConf)
//      )
//    }
//
//    (info: WorkerInitInfo) => {
//      val namedContext = mkNamedContext(info)
//      val (h, p) = info.masterHttpConf.split(':') match {
//        case Array(host, port) => (host, port.toInt)
//      }
//      val artifactDownloader = ArtifactDownloader.create(h, p, info.jobsSavePath)
//      val runnerSelector = new SimpleRunnerSelector
//      val props = WorkerActor.props(runnerSelector, namedContext, artifactDownloader, info.downtime, info.maxJobs)
//      (namedContext, props)
//    }
//  }
//
//}
