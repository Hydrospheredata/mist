package io.hydrosphere.mist.worker

import java.io.File
import java.util.concurrent.Executors

import akka.actor._
import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.worker.runners.{ArtifactDownloader, JobRunner, RunnerSelector, SimpleRunnerSelector}
import mist.api.data.JsLikeData
import org.apache.spark.streaming.StreamingContext
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
  val artifactDownloader: ArtifactDownloader
) extends Actor with JobStarting with ActorLogging {

  implicit val ec = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val service = Executors.newFixedThreadPool(1)
    ExecutionContext.fromExecutorService(
      service,
      e => logger.error("Error from thread pool", e)
    )
  }

  override def receive: Receive = awaitRequest()

  private def awaitRequest(): Receive = {
    case req: RunJobRequest =>
      val jobStarted = startJob(req)
      val unit = ExecutionUnit(sender(), jobStarted)
      context become running(unit)

    case CompleteAndShutdown =>
      self ! PoisonPill
  }

  private def running(execution: ExecutionUnit): Receive = {
    case RunJobRequest(id, _, _) =>
      sender() ! WorkerIsBusy(id)
    case resp: JobResponse =>
      log.info(s"Job execution done. Returning result $resp and become awaiting new request")
      execution.requester ! resp
      context become awaitRequest()

    case CancelJobRequest(id) => cancel(id, sender())

    case CompleteAndShutdown =>
      context become completeAndShutdown(execution)
  }

  private def completeAndShutdown(execution: ExecutionUnit): Receive = {
    case CancelJobRequest(id) => cancel(id, sender())
    case resp: JobResponse =>
      log.info(s"Job execution done. Returning result $resp and shutting down")
      execution.requester ! resp
      self ! PoisonPill
  }

  private def cancel(id: String, respond: ActorRef): Unit = {
    namedContext.sparkContext.cancelJobGroup(id)
    StreamingContext.getActive().foreach( _.stop(stopSparkContext = false, stopGracefully = true))
    respond ! JobIsCancelled(id)
    context become awaitRequest()
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
    runnerSelector: RunnerSelector
  ): Props =
    Props(new WorkerActor(runnerSelector, context, artifactDownloader))

  def props(
    context: NamedContext,
    artifactDownloader: ArtifactDownloader
  ): Props =
    Props(new WorkerActor(new SimpleRunnerSelector, context, artifactDownloader))
}

