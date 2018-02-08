package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorSystem
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Mix connections mirror and connector starter
  */
class WorkerHub(
  mkConnector: (String, ContextConfig) => WorkerConnector
) extends ConnectionsMirror  {

  def start(id: String, ctx: ContextConfig): WorkerConnector = {
    val connector = mkConnector(id, ctx)
    spy(connector)
  }

  // stopping connection return failed future
  // because it stops worker focibly
  // calling stop from here means that user understands it
  private def shutdownConn(conn: WorkerConnection): Future[Unit] =
    conn.shutdown().recover({case _ => ()})

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
    val mkConnector = WorkerConnector.default(regHub, spawn, system)(_, _)
    new WorkerHub(mkConnector)
  }
}


