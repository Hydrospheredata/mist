package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.Logger

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util._

import scala.concurrent.ExecutionContext.Implicits.global
/**
  * Collects alive worker connections
  */
trait WorkersMirror extends Logger { self =>

  import WorkersMirror._

  private val workersMap = TrieMap.empty[String, WorkerLink]

  private def add(workerLink: WorkerLink): Unit = workersMap.update(workerLink.name, workerLink)
  private def remove(id: String): Unit = workersMap.remove(id)

  def workers(): Seq[WorkerLink] = workersMap.values.toSeq

  def worker(id: String): Option[WorkerLink] = workersMap.get(id)

  def spy(connector: WorkerConnector): WorkerConnector = spiedConnector(self, connector)

}

object WorkersMirror extends Logger {

  /**
    * Used for spying over connectors and report created connections to mirror
    */
  class MirrorSpyConnector(
    mirror: WorkersMirror,
    underlying: WorkerConnector
  ) extends WorkerConnector {

    override def shutdown(force: Boolean): Future[Unit] = underlying.shutdown(force)
    override def whenTerminated(): Future[Unit] = underlying.whenTerminated()
    override def askConnection(): Future[WorkerConnection] = {
      underlying.askConnection().andThen {
        case Success(conn) => spy(conn)
        case _ =>
      }
    }

    private def spy(conn: WorkerConnection): Unit = {
      mirror.add(conn.data)
      conn.whenTerminated.onComplete(_ => mirror.remove(conn.data.name))
    }
  }

  private def spiedConnector(
    mirror: WorkersMirror,
    connector: WorkerConnector
  ): WorkerConnector = new MirrorSpyConnector(mirror, connector)

}

class StandaloneWorkersMirror extends WorkersMirror

