package io.hydrosphere.mist.master.execution.remote

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import io.hydrosphere.mist.core.CommonData.{WorkerInitInfo, WorkerReady}
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.{Future, Promise}

object Executor {

  class RemoteInitializer(
    ctx: ContextConfig,
    ready: Promise[Unit],
    remote: ActorRef
  ) extends Actor with ActorLogging {

    override def preStart = {
      context watch remote
      log.info("SEND INIT DATA")
      remote ! WorkerInitInfo(
        sparkConf = ctx.sparkConf,
        maxJobs = ctx.maxJobs,
        downtime = ctx.downtime,
        streamingDuration = ctx.streamingDuration,
        logService = "localhost:2005",
        masterHttpConf = "localhost:2004",
        jobsSavePath = "/tmp"
      )
    }

    private def waitInit: Receive = {
      case WorkerReady =>
        log.info("worker ready")
        ready.success(())
        context become proxy
      case Terminated(_) =>
        ready.failure(new RuntimeException("???"))
        log.info("remote was terminated")
        context.stop(self)
    }

    private def proxy: Receive = {
      case Terminated(_) =>
        log.info("remote was terminated")
        context.stop(self)
      case x =>
        log.info(s"FORWARD $x")
        remote forward x
    }

    override def receive: Receive = waitInit
  }

  object RemoteInitializer {
    def props(ctx: ContextConfig, ready: Promise[Unit], remote: ActorRef): Props =
      Props(classOf[RemoteInitializer], ctx, ready, remote)
  }

  def default(
    regHub: ActorRegHub,
    spawnSettings: SpawnSettings,
    af: ActorRefFactory
  ): (String, ContextConfig) => Future[ActorRef] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    (id: String, ctx: ContextConfig) => {
      spawnSettings.runner.runWorker(id, ctx, RunMode.Shared)
      regHub.waitRef(id, spawnSettings.timeout).flatMap(ref => {
        val ready = Promise[Unit]
        val proxy = af.actorOf(RemoteInitializer.props(ctx, ready, ref))
        ready.future.map(_ => proxy)
      })
    }
  }

}
