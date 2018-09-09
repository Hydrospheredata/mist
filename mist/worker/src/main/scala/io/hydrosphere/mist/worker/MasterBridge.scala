package io.hydrosphere.mist.worker

import java.nio.file.Path

import akka.actor._
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax, ActorRegHub}
import io.hydrosphere.mist.worker.MasterBridge.{AppShutdown, ReceiveInitTimeout}
import io.hydrosphere.mist.worker.logging.RemoteLogsWriter
import io.hydrosphere.mist.worker.runners.ArtifactDownloader

import scala.concurrent.duration._

class MasterBridge(
  id: String,
  regHub: ActorRef,
  mkContext: WorkerInitInfo => MistScContext,
  workerF: ActorF[(WorkerInitInfo, MistScContext)]
) extends Actor with ActorLogging with Timers with ActorFSyntax {

  val initTimerKey = s"$id-receive-init-data"

  override def preStart: Unit = {
    regHub ! ActorRegHub.Register(id)
    log.info("send registration to {}", regHub)
    timers.startSingleTimer(initTimerKey, ReceiveInitTimeout, 1 minute)
  }

  override def receive: Receive = waitInit

  private def waitInit: Receive = {
    def createContext(initInfo: WorkerInitInfo): Either[Throwable, MistScContext] = {
      try {
        Right(mkContext(initInfo))
      } catch {
        case e: Throwable => Left(e)
      }
    }

    {
      case AppShutdown =>
        log.info("Receive shutdown before initialization")
        context stop self

      case init:WorkerInitInfo =>
        timers.cancel(initTimerKey)
        log.info("Received init info, {}", init)
        val remoteConnection = sender()
        createContext(init) match {
          case Left(e) =>
            log.error(e, "Couldn't create spark context")
            val msg = s"Spark context instantiation failed, ${e.getMessage}"
            remoteConnection ! WorkerStartFailed(id, msg)
            context stop self

          case Right(ctx) =>
            val worker = workerF.create(init, ctx)
            val sparkUI = ctx.getUIAddress()
            context watch remoteConnection
            context watch worker
            log.info("Become work")
            remoteConnection ! WorkerReady(id, sparkUI)
            context become work(sender(), worker)
        }

      case ReceiveInitTimeout =>
        log.error("Initial data wasn't received for a minutes - shutdown")
        context stop self
      }
  }

  private def work(remote: ActorRef, worker: ActorRef): Receive = {
    def goToAwaitTermination(): Unit = {
      remote ! RequestTermination
      context.setReceiveTimeout(30 seconds)
      context become awaitTermination(remote)
    }
    {
      case Terminated(ref) if ref == remote =>
        log.warning("Remote connection was terminated - shutdown")
        context unwatch worker
        worker ! PoisonPill
        context stop self

      case Terminated(ref) if ref == worker =>
        log.info("Underlying worker was terminated - request termination")
        goToAwaitTermination()

      case ShutdownWorker =>
        context unwatch worker
        worker ! PoisonPill
        goToAwaitTermination()

      case AppShutdown =>
        log.error(s"Unexpectedly received application shutdown command")
        context unwatch worker
        worker ! PoisonPill
        goToAwaitTermination()

      case x => worker forward x
    }
  }

  private def awaitTermination(remote: ActorRef): Receive = {
    case AppShutdown | ShutdownWorkerApp =>
      log.info(s"Received application shutdown command")
      remote ! Goodbye
      context stop self

    case Terminated(ref) if ref == remote =>
      log.warning("Remote connection was terminated - shutdown")
      context stop self

    case ReceiveTimeout =>
      log.error("Didn't receive any stop command - shutdown")
      context stop self
  }

}

object MasterBridge {

  case object ReceiveInitTimeout
  case object AppShutdown

  def props(
    id: String,
    regHub: ActorRef,
    mkContext: WorkerInitInfo => MistScContext,
    workerF: ActorF[(WorkerInitInfo, MistScContext)]
  ): Props = {
    Props(classOf[MasterBridge], id, regHub, mkContext, workerF)
  }

  def props(
    id: String,
    workerDir: Path,
    regHub: ActorRef
  ): Props = {

    val mkContext = (info: WorkerInitInfo) => MistScContext(id, info.streamingDuration)

    val workerF = ActorF[(WorkerInitInfo, MistScContext)]({ case ((init, ctx), af) =>
      val hostPort = init.masterHttpConf.split(":")
      val downloader = ArtifactDownloader.create(
        hostPort(0), hostPort(1).toInt, init.maxArtifactSize, workerDir
      )
      val writer = {
        val hostPort = init.logService.split(":")
        RemoteLogsWriter.getOrCreate(hostPort(0), hostPort(1).toInt)
      }
      af.actorOf(WorkerActor.props(ctx, downloader, RequestSetup.loggingSetup(writer)))
    })
    props(id, regHub, mkContext, workerF)
  }

}

