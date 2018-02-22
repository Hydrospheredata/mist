package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait WorkerRunner extends ((String, ContextConfig) => Future[WorkerConnection])

object WorkerRunner {

  class DefaultRunner(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    connect: (String, WorkerInitInfo, FiniteDuration, ActorRef) => Future[WorkerConnection]
  ) extends WorkerRunner {

    override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      import spawn._

      runnerCmd.runWorker(id, ctx)
      val initInfo = toWorkerInitInfo(ctx)
      val future = for {
        ref <- regHub.waitRef(id, timeout)
        connection <- connect(id, initInfo, readyTimeout, ref)
      } yield {
        connection.whenTerminated.onComplete(_ => {
          runnerCmd.onStop(id)
        })
        connection
      }

      future.onFailure({case _ => runnerCmd.onStop(id)})
      future
    }

  }

  def default(spawn: SpawnSettings, regHub: ActorRegHub, af: ActorRefFactory): WorkerRunner = {
    val connect = (id: String, info: WorkerInitInfo, ready: FiniteDuration, remote: ActorRef) => {
      WorkerBridge.connect(id, info, ready, remote)(af)
    }
    new DefaultRunner(spawn, regHub, connect)
  }

}
