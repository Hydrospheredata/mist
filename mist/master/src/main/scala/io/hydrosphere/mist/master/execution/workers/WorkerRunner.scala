package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorRefFactory
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait WorkerRunner extends ((String, ContextConfig) => Future[WorkerConnection])

object WorkerRunner {

  class DefaultRunner(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    af: ActorRefFactory
  ) extends WorkerRunner {

    override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      import spawn._

      runner.runWorker(id, ctx)
      val initInfo = toWorkerInitInfo(ctx)
      for {
        ref <- regHub.waitRef(id, timeout)
        connection <- WorkerBridge.connect(id, initInfo, readyTimeout, ref)(af)
      } yield {
        connection.whenTerminated.onComplete(_ => {
          runner.onStop(id)
        })
        connection
      }
    }

  }

  def default(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    af: ActorRefFactory
  ): WorkerRunner = new DefaultRunner(spawn, regHub, af)

}
