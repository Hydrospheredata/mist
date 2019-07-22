package io.hydrosphere.mist.worker

import java.io.{PrintWriter, StringWriter}

import akka.actor._
import akka.pattern.pipe
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.worker.runners._
import mist.api.data.JsData
import org.apache.spark.streaming.StreamingContext

import scala.concurrent.Future

import RequestSetup._

class WorkerActor(
  namedContext: MistScContext,
  artifactDownloader: ArtifactDownloader,
  requestSetup: ReqSetup,
  runnerSelector: RunnerSelector
) extends Actor with ActorLogging {

  import context._

  type JobFuture = CancellableFuture[JsData]
  type CleanUp = RunJobRequest => Unit

  override def receive: Receive = awaitRequest()

  private def awaitRequest(): Receive = {
    case req: RunJobRequest =>
      val cleanUp = requestSetup(req)
      sender() ! JobFileDownloading(req.id)
      artifactDownloader.downloadArtifact(req.params.filePath) pipeTo self
      context become downloading(sender(), req, cleanUp)
  }

  private def downloading(
    respond: ActorRef,
    req: RunJobRequest,
    cleanUp: CleanUp
  ): Receive = {
    case artifact: SparkArtifact =>
      val runner = runnerSelector.selectRunner(artifact)
      namedContext.sc.setJobGroup(req.id, req.id)
      val jobFuture = run(runner, req)
      respond ! JobStarted(req.id)
      jobFuture.future pipeTo self
      context become running(req, respond, jobFuture, cleanUp)

    case CancelJobRequest(id) =>
      cleanUp(req)
      respond ! JobIsCancelled(id)
      respond ! mkFailure(req, new RuntimeException("Job was cancelled before starting"))
      context become awaitRequest()

    case Status.Failure(e) =>
      respond ! mkFailure(req, e)
      context become awaitRequest()
  }

  private def running(
    req: RunJobRequest,
    respond: ActorRef,
    jobFuture: JobFuture,
    cleanUp: CleanUp
  ): Receive = {
    case req: RunJobRequest =>
      sender() ! WorkerIsBusy(req.id)

    case data: JsData =>
      log.info(s"Job execution done. Returning result $data and become awaiting new request")
      cleanUp(req)
      respond ! JobSuccess(req.id, data)
      context become awaitRequest()

    case Status.Failure(e) =>
      log.info(s"Job execution done. Returning result $e and become awaiting new request")
      cleanUp(req)
      respond ! mkFailure(req, e)
      context become awaitRequest()

    case CancelJobRequest(id) =>
      cancel(req, sender(), jobFuture)
  }

  private def cancel(req: RunJobRequest, respond: ActorRef, jobFuture: JobFuture): Unit = {
    namedContext.sc.cancelJobGroup(req.id)
    StreamingContext.getActive().foreach( _.stop(stopSparkContext = false, stopGracefully = true))
    jobFuture.cancel()
    respond ! JobIsCancelled(req.id)
  }

  override def postStop(): Unit = {
    artifactDownloader.stop()
    namedContext.stop()
  }

  private def run(runner: JobRunner, req: RunJobRequest): JobFuture = CancellableFuture.onDetachedThread {
    runner.run(req, namedContext) match {
      case Left(e: InterruptedException) => throw new RuntimeException("Execution was cancelled")
      case Left(e) => throw e
      case Right(data) => data
    }
  }

  private def mkFailure(req: RunJobRequest, ex: Throwable): JobResponse =
    JobFailure(req.id, buildErrorMessage(ex))

  protected def buildErrorMessage(e: Throwable): String = {
    val writer = new StringWriter()
    e.printStackTrace(new PrintWriter(writer))
    writer.toString
  }
}

object WorkerActor {

  def props(
    context: MistScContext,
    artifactDownloader: ArtifactDownloader,
    requestSetup: ReqSetup,
    runnerSelector: RunnerSelector
  ): Props =
    Props(classOf[WorkerActor], context, artifactDownloader, requestSetup, runnerSelector)

  def props(context: MistScContext, artifactDownloader: ArtifactDownloader, requestSetup: ReqSetup): Props =
    props(context, artifactDownloader, requestSetup, new SimpleRunnerSelector)

}

