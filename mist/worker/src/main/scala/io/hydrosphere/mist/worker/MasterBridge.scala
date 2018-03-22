package io.hydrosphere.mist.worker

import java.nio.file.Path

import akka.actor._
import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax, ActorRegHub}
import io.hydrosphere.mist.worker.MasterBridge.ReceiveInitTimeout
import io.hydrosphere.mist.worker.runners.ArtifactDownloader
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration._

class MasterBridge(
  id: String,
  regHub: ActorRef,
  mkContext: WorkerInitInfo => NamedContext,
  workerF: ActorF[(WorkerInitInfo, NamedContext)]
) extends Actor with ActorLogging with Timers with ActorFSyntax {

  val initTimerKey = s"$id-receive-init-data"

  override def preStart: Unit = {
    regHub ! ActorRegHub.Register(id)
    log.info("send registration to {}", regHub)
    timers.startSingleTimer(initTimerKey, ReceiveInitTimeout, 1 minute)
  }

  override def receive: Receive = waitInit

  private def waitInit: Receive = {
    def createContext(initInfo: WorkerInitInfo): Either[Throwable, NamedContext] = {
      try {
        Right(mkContext(initInfo))
      } catch {
        case e: Throwable => Left(e)
      }
    }

    {
      case init:WorkerInitInfo =>
        timers.cancel(initTimerKey)
        log.info("Received init info, {}", init)
        val remoteConnection = sender()
        createContext(init) match {
          case Left(e) =>
            log.error(e, "Couldn't create spark context")
            val msg = s"Spark context instantiation failed, ${e.getMessage}"
            remoteConnection ! WorkerStartFailed(id, msg)
            shutdown()

          case Right(ctx) =>
            val worker = workerF.create(init, ctx)
            val sparkUI = SparkUtils.getSparkUiAddress(ctx.sparkContext)
            context watch remoteConnection
            context watch worker
            log.info("Become work")
            remoteConnection ! WorkerReady(id, sparkUI)
            context become work(sender(), worker)
        }

      case ReceiveInitTimeout =>
        log.error("Initial data wasn't received for a minutes - shutdown")
        shutdown()
      }
  }


  private def work(remote: ActorRef, worker: ActorRef): Receive = {
    case Terminated(ref) if ref == remote =>
      log.warning("Remote connection was terminated - shutdown")
      worker ! PoisonPill
      shutdown()
    case Terminated(ref) if ref == worker =>
      log.info("Underlying worker was terminated - shutdown")
      shutdown()

    case ForceShutdown =>
      log.info("Received force shutdown")
      worker ! PoisonPill
      shutdown()

    case x => worker forward x
  }

  private def shutdown(): Unit = {
    context stop self
    context.system.terminate()
  }
}

object MasterBridge {

  case object ReceiveInitTimeout

  def props(
    id: String,
    regHub: ActorRef,
    mkContext: WorkerInitInfo => NamedContext,
    workerF: ActorF[(WorkerInitInfo, NamedContext)]
  ): Props = {
    Props(classOf[MasterBridge], id, regHub, mkContext, workerF)
  }

  def props(
    id: String,
    workerDir: Path,
    regHub: ActorRef
  ): Props = {
    val mkContext = createNamedContext(id)(_)
    val workerF = ActorF[(WorkerInitInfo, NamedContext)]({ case ((init, ctx), af) =>
      val hostPort = init.masterHttpConf.split(":")
      val downloader = ArtifactDownloader.create(
        hostPort(0), hostPort(1).toInt, init.maxArtifactSize, workerDir
      )
      af.actorOf(WorkerActor.props(ctx, downloader))
    })
    props(id, regHub, mkContext, workerF)
  }

  def createNamedContext(id: String)(init: WorkerInitInfo): NamedContext = {
    val conf = new SparkConf()
      .setAppName(id)
      .setAll(init.sparkConf)
      .set("spark.streaming.stopSparkContextByDefault", "false")
    val sparkContext = new SparkContext(conf)

    val centralLoggingConf = {
      val hostPort = init.logService.split(":")
      CentralLoggingConf(hostPort(0), hostPort(1).toInt)
    }

    new NamedContext(
      sparkContext,
      id,
      Option(centralLoggingConf),
      org.apache.spark.streaming.Duration(init.streamingDuration.toMillis)
    )
  }
}

