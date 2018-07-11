package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.execution.workers.starter.{WorkerProcess, WorkerStarter}
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
    starter: WorkerStarter,
    regHub: ActorRegHub,
    connect: (String, WorkerInitInfo, FiniteDuration, ActorRef, StopAction) => Future[WorkerConnection]
  ) extends WorkerRunner {

    override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      import spawn._

      val initInfo = toWorkerInitInfo(ctx)

      def continueSetup(ps: WorkerProcess.StartedProcess): Future[WorkerConnection] ={
        val regFuture = for {
          ref <- regHub.waitRef(id, timeout)
          connection <- connect(id, initInfo, readyTimeout, ref, starter.stopAction)
        } yield connection

        val promise = Promise[WorkerConnection]
        ps match {
          case WorkerProcess.Local(term) =>
            term.onFailure({case e => promise.tryComplete(Failure(new RuntimeException(s"Process terminated with error $e")))})
          case WorkerProcess.NonLocal =>
        }

        starter.stopAction match {
          case StopAction.CustomFn(f) => regFuture.onFailure({case _ => f(id)})
          case StopAction.Remote =>
        }

        regFuture.onComplete(r => promise.tryComplete(r))
        promise.future
      }


      starter.onStart(id, initInfo) match {
        case ps: WorkerProcess.StartedProcess => continueSetup(ps)
        case WorkerProcess.Failed(e) => Future.failed(new RuntimeException("Starting worker failed", e))
      }
    }

  }

  def default(spawn: SpawnSettings, starter: WorkerStarter, regHub: ActorRegHub, af: ActorRefFactory): WorkerRunner = {
    val connect = (id: String, info: WorkerInitInfo, ready: FiniteDuration, remote: ActorRef, stopAction: StopAction) => {
      WorkerBridge.connect(id, info, ready, remote, stopAction)(af)
    }
    new DefaultRunner(spawn, starter, regHub, connect)
  }

}
