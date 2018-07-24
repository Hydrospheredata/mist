package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

sealed trait InfoPart
object InfoPart {
  case class Text(text: String) extends InfoPart
  case class Link(text: String, href: String) extends InfoPart
}

case class ClusterInfo(
  id: String,
  `type`: String,
  data: Seq[InfoPart]
)

trait Connector {
  def askConnection(): Future[PerJobConnection]
  def shutdown(force: Boolean): Future[Unit]
  def whenTerminated(): Future[Unit]
  def describe(): ClusterInfo
}

trait Cluster extends Connector {
  def workers(): Future[Seq[WorkerLink]]
}

class LocalCluster(connManager: ConnectionManager) extends Cluster {

  private val workersMap = TrieMap.empty[String, WorkerConnection]

  override def askConnection(): Future[PerJobConnection] = connManager.askConnection()
  override def shutdown(force: Boolean): Future[Unit] = connManager.shutdown(force)
  override def whenTerminated(): Future[Unit] = connManager.whenTerminated()
  override def workers(): Future[Seq[WorkerLink]] = ???
  override def describe(): ClusterInfo = ???
}

class EMRCluster(
  connManager: ConnectionManager
)

