package io.hydrosphere.mist.worker

import java.util.concurrent.Executors

import akka.actor._
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.worker.runners._
import mist.api.data.JsLikeData
import org.apache.log4j.{Appender, LogManager}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import scala.concurrent._
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
  protected val rootLogger = LogManager.getRootLogger


  protected final def startJob(
    req: RunJobRequest
  )(implicit ec: ExecutionContext): Future[Either[Throwable, JsLikeData]] = {
    val id = req.id
    val s = sender()
    val jobStart = for {
      artifact <- downloadFile(s, req)
      runner   =  runnerSelector.selectRunner(artifact)
      res      =  runJob(s, req, runner)
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

  private def downloadFile(actor: ActorRef, req: RunJobRequest): Future[SparkArtifact] = {
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
  mkAppender: String => Option[Appender]
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
      mkAppender(req.id).foreach(rootLogger.addAppender)
      val jobStarted = startJob(req)
      val unit = ExecutionUnit(sender(), jobStarted)
      context become running(unit)

    case _: ShutdownCommand =>
      self ! PoisonPill
  }

  private def running(execution: ExecutionUnit): Receive = {
    case RunJobRequest(id, _, _) =>
      sender() ! WorkerIsBusy(id)
    case resp: JobResponse =>
      log.info(s"Job execution done. Returning result $resp and become awaiting new request")
      rootLogger.removeAppender(resp.id)
      execution.requester ! resp
      context become awaitRequest()

    case CancelJobRequest(id) =>
      cancel(id, sender())
      context become awaitRequest()

    case ForceShutdown =>
      self ! PoisonPill

    case CompleteAndShutdown =>
      context become completeAndShutdown(execution)
  }

  private def completeAndShutdown(execution: ExecutionUnit): Receive = {
    case CancelJobRequest(id) =>
      cancel(id, sender())
      self ! PoisonPill
    case resp: JobResponse =>
      log.info(s"Job execution done. Returning result $resp and shutting down")
      rootLogger.removeAppender(resp.id)
      execution.requester ! resp
      self ! PoisonPill
  }

  private def cancel(id: String, respond: ActorRef): Unit = {
    rootLogger.removeAppender(id)
    namedContext.sparkContext.cancelJobGroup(id)
    StreamingContext.getActive().foreach( _.stop(stopSparkContext = false, stopGracefully = true))
    respond ! JobIsCancelled(id)
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
    runnerSelector: RunnerSelector,
    mkAppender: String => Option[Appender]
  ): Props =
    Props(new WorkerActor(runnerSelector, context, artifactDownloader, mkAppender))

  def props(context: NamedContext, artifactDownloader: ArtifactDownloader, runnerSelector: RunnerSelector): Props =
    props(context, artifactDownloader, runnerSelector, mkAppenderF(context.loggingConf))

  def props(context: NamedContext, artifactDownloader: ArtifactDownloader): Props =
    props(context, artifactDownloader, new SimpleRunnerSelector)

  def mkAppenderF(conf: Option[CentralLoggingConf]): String => Option[Appender] =
    (id: String) => conf.map(c => RemoteAppender(id, c))

}

