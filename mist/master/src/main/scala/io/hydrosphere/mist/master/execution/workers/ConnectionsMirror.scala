package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.utils.Logger

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util._

/**
  * Collects alive worker connections
  */
trait ConnectionsMirror extends Logger { self =>

  import ConnectionsMirror._

  private val workersMap = TrieMap.empty[String, WorkerConnection]

  private def add(conn: WorkerConnection): Unit = workersMap.update(conn.data.name, conn)
  private def remove(id: String): Unit = workersMap.remove(id)

  def workerConnections(): Seq[WorkerConnection] = workersMap.values.toSeq

  def workerConnection(id: String): Option[WorkerConnection] = workersMap.get(id)

  def spy(connector: WorkerConnector): WorkerConnector = spiedConnector(self, connector)

}

object ConnectionsMirror extends Logger {

  /**
    * Used for spying over connectors and report created connections to mirror
    */
  class MirrorSpyConnector(
    mirror: ConnectionsMirror,
    underlying: WorkerConnector
  ) extends WorkerConnector {

    override def shutdown(force: Boolean): Future[Unit] = underlying.shutdown(force)
    override def warmUp(): Unit = underlying.warmUp()
    override def whenTerminated(): Future[Unit] = underlying.whenTerminated()
    override def askConnection(): Future[WorkerConnection] = {
      underlying.askConnection().andThen {
        case Success(conn) => spy(conn)
        case _ =>
      }
    }

    private def spy(conn: WorkerConnection): Unit = {
      mirror.add(conn)
      conn.whenTerminated.onComplete(_ => mirror.remove(conn.data.name))
    }
  }

  private def spiedConnector(
    mirror: ConnectionsMirror,
    connector: WorkerConnector
  ): WorkerConnector = new MirrorSpyConnector(mirror, connector)

  def standalone(): ConnectionsMirror = new ConnectionsMirror {}

}

