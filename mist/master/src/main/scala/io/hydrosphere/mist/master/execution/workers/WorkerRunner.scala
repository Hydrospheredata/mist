package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

import scala.util._

trait WorkerRunner extends ((String, ContextConfig) => Future[WorkerConnection])

object WorkerRunner {

  class DefaultRunner(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    connect: (String, WorkerInitInfo, FiniteDuration, ActorRef) => Future[WorkerConnection]
  ) extends WorkerRunner {

    override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      import spawn._

      val initInfo = toWorkerInitInfo(ctx)
      val ps = runnerCmd.onStart(id, initInfo)
      val regFuture = for {
        ref <- regHub.waitRef(id, timeout)
        connection <- connect(id, initInfo, readyTimeout, ref)
      } yield {
        connection.whenTerminated.onComplete(_ => {
          runnerCmd.onStop(id)
        })
        connection
      }

      regFuture.onFailure({case _ => runnerCmd.onStop(id)})

      val promise = Promise[WorkerConnection]
      ps match {
        case Local(term) =>
          term.onFailure({case e => promise.tryComplete(Failure(new RuntimeException(s"Process terminated with error $e")))})
        case NonLocal =>
      }
      regFuture.onComplete(r => promise.tryComplete(r))

      promise.future
    }

  }

  def default(spawn: SpawnSettings, regHub: ActorRegHub, af: ActorRefFactory): WorkerRunner = {
    val connect = (id: String, info: WorkerInitInfo, ready: FiniteDuration, remote: ActorRef) => {
      WorkerBridge.connect(id, info, ready, remote)(af)
    }
    new DefaultRunner(spawn, regHub, connect)
  }

}
