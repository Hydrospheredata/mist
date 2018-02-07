package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

trait WorkerConnector {

  def askConnection(): Future[WorkerConnection]

  def shutdown(force: Boolean): Future[Unit]

  def whenTerminated(): Future[Unit]

}

object WorkerConnector extends Logger {

  sealed trait Event
  object Event {
    final case class AskConnection(resolve: Promise[WorkerConnection]) extends Event
    final case class Shutdown(force: Boolean) extends Event
  }

  def default(
    regHub: ActorRegHub,
    spawnSettings: SpawnSettings,
    af: ActorRefFactory
  ): (String, ContextConfig) => WorkerConnector = {

    import scala.concurrent.ExecutionContext.Implicits.global

    def mkWorkerInitInfo(ctx: ContextConfig): WorkerInitInfo = {
      WorkerInitInfo(
        sparkConf = ctx.sparkConf,
        maxJobs = ctx.maxJobs,
        downtime = ctx.downtime,
        streamingDuration = ctx.streamingDuration,
        logService = "localhost:2005",
        masterHttpConf = "localhost:2004",
        jobsSavePath = "/tmp"
      )
    }

    def startWorker(mode: RunMode)(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      spawnSettings.runner.runWorker(id, ctx, mode)
      regHub.waitRef(id, spawnSettings.timeout).flatMap(ref => {
        val ready = Promise[WorkerConnection]
        af.actorOf(WorkerBridge.props(id, mkWorkerInitInfo(ctx), ref, ready, 1 minute))
        ready.future
      })
    }

    def mkConnector(mode: RunMode, connActror: ActorRef): WorkerConnector = {
      new WorkerConnector {
        override def whenTerminated(): Future[Unit] = Promise[Unit].future

        override def askConnection(): Future[WorkerConnection] = {
          val promise = Promise[WorkerConnection]
          connActror ! WorkerConnector.Event.AskConnection(promise)
          promise.future
        }

        override def shutdown(force: Boolean): Future[Unit] = ???
      }
    }

    (id: String, ctx: ContextConfig) => {
      ctx.workerMode match {
        case "shared" => mkConnector(RunMode.Shared, af.actorOf(SharedConnector.props(id, ctx, startWorker(RunMode.Shared))))
        case "exclusive" => mkConnector(RunMode.ExclusiveContext(None), af.actorOf(ExclusiveConnector.props(id, ctx, startWorker(RunMode.ExclusiveContext(None)))))
        case unknown => throw new IllegalArgumentException(s"Unknown worker mode: $unknown")
      }
    }

  }

}

