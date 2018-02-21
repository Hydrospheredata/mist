package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorSystem
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
  * Mix connections mirror and connector starter
  */
class WorkerHub(
  runner: WorkerRunner,
  mkConnector: (String, ContextConfig, WorkerRunner) => WorkerConnector
) extends ConnectionsMirror  {

  private val wrappedRunner = new WorkerRunner {
    override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] = {
      val pr = Promise[WorkerConnection]
      runner(id, ctx).onComplete {
        case Success(conn) =>
          add(conn)
          conn.whenTerminated.onComplete({_ => remove(conn.id)})
          pr.success(conn)
        case Failure(e) => pr.failure(e)
      }
      pr.future
    }

  }

  def start(id: String, ctx: ContextConfig): WorkerConnector = mkConnector(id, ctx, wrappedRunner)

  // stopping connection return failed future
  // because it stops worker forcibly
  // calling stop from here means that user understands it
  private def shutdownConn(conn: WorkerConnection): Future[Unit] =
    conn.shutdown(true).recover({case _ => ()})

  def shutdownWorker(id: String): Future[Unit] = workerConnection(id) match {
    case Some(connection) => shutdownConn(connection)
    case None => Future.failed(new IllegalStateException(s"Unknown worker $id"))
  }

  def shutdownAllWorkers(): Future[Unit] =
    Future.sequence(workerConnections().map(conn => shutdownConn(conn))).map(_ => ())

}

object WorkerHub {

  def apply(spawn: SpawnSettings, system: ActorSystem): WorkerHub = {
    val regHub = ActorRegHub("regHub", system)
    val runner =  WorkerRunner.default(spawn, regHub, system)
    val mkConnector = (id: String, ctx: ContextConfig, runner: WorkerRunner) => {
      WorkerConnector.actorBased(id,ctx, runner, system)
    }
    new WorkerHub(runner, mkConnector)
  }
}


