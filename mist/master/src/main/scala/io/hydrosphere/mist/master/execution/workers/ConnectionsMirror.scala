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

  private val workersMap = TrieMap.empty[String, WorkerConnection]

  private [workers] def add(conn: WorkerConnection): Unit = workersMap.update(conn.data.name, conn)
  private [workers] def remove(id: String): Unit = workersMap.remove(id)

  def workerConnections(): Seq[WorkerConnection] = workersMap.values.toSeq

  def workerConnection(id: String): Option[WorkerConnection] = workersMap.get(id)

}

object ConnectionsMirror extends Logger {

  def standalone(): ConnectionsMirror = new ConnectionsMirror {}

}

