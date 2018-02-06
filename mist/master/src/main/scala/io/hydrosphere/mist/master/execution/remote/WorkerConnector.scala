package io.hydrosphere.mist.master.execution.remote

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.akka.{ActorRegHub, WhenTerminated}

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
  ): (String, ContextConfig) => Future[WorkerConnector] = {

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

    def forShared(id: String, ctx: ContextConfig): Future[WorkerConnector] = {
      spawnSettings.runner.runWorker(id, ctx, RunMode.Shared)
      regHub.waitRef(id, spawnSettings.timeout).flatMap(ref => {
        val ready = Promise[ActorRef]
        af.actorOf(WorkerBridge.props(id, mkWorkerInitInfo(ctx), ref, ready, 1 minute))
        ready.future
      }).map(connection => {
        //TODO shutdown
        new WorkerConnector {
          override def askConnection(): Future[WorkerConnection] = Future.successful(WorkerConnection(id, connection))

          override def shutdown(force: Boolean): Future[Unit] = ???

          override def whenTerminated(): Future[Unit] = {
            val promise = Promise[Unit]
            WhenTerminated(connection, { promise.success(()) })(af)
            promise.future
          }
        }
      })
    }

    def forExclusive(id: String, ctx: ContextConfig): Future[WorkerConnector] = {
      val startWorker = (id: String, ctx: ContextConfig) => {
        spawnSettings.runner.runWorker(id, ctx, RunMode.ExclusiveContext(None))
        regHub.waitRef(id, spawnSettings.timeout).flatMap(ref => {
          val ready = Promise[ActorRef]
          af.actorOf(WorkerBridge.props(id, mkWorkerInitInfo(ctx), ref, ready, 1 minute))
          ready.future
        })
      }
      val props = ExclusiveConnector.props(id, ctx, startWorker)
      val actor = af.actorOf(props)
      val connector = new WorkerConnector {
        //todo
        override def whenTerminated(): Future[Unit] = Promise[Unit].future

        override def askConnection(): Future[WorkerConnection] = {
          val promise = Promise[WorkerConnection]
          actor ! WorkerConnector.Event.AskConnection(promise)
          promise.future
        }

        override def shutdown(force: Boolean): Future[Unit] = ???
      }
      Future.successful(connector)
    }

    (id: String, ctx: ContextConfig) => {
      ctx.workerMode match {
        case "shared" => forShared(id, ctx)
        case "exclusive" => forExclusive(id, ctx)
        case unknown => Future.failed(new IllegalArgumentException(s"Unknown worker mode: $unknown"))
      }
    }

  }

}

