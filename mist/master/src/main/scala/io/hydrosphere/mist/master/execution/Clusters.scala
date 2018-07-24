package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorSystem}
import io.hydrosphere.mist.master.EMRProvisionerConfig
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection, WorkerRunner}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

trait ConnectionManager {
  def askConnection(): Future[WorkerConnection]
  def shutdown(force: Boolean): Future[Unit]
  def whenTerminated(): Future[Unit]
}

trait WorkerLauncher {
  def launch(name: String, ctx: ContextConfig): Future[WorkerConnection]
}

trait FutureRef[A, B] {
  def value: A
  def future: Future[B]
}

object FutureRef {

  class FromPromise[A, B](v: A, p: Promise[B]) extends FutureRef[A, B] {
    override def value: A = v
    override def future: Future[B] = p.future
  }

  def apply[A, B](value: A, promise: Promise[B]): FutureRef[A, B] = new FromPromise(value, promise)
}

trait ClusterStopper {
  def stop(): Future[Unit]
}

trait ClusterLauncher {
  def launch(ctx: ContextConfig): FutureRef[ClusterStopper, Cluster]
}

object ClusterLauncher {

  class LocalCluster(connManager: ConnectionManager) extends Cluster {

    private val workersMap = TrieMap.empty[String, WorkerConnection]

    override def askConnection(): Future[PerJobConnection] = connManager.askConnection()
    override def shutdown(force: Boolean): Future[Unit] = connManager.shutdown(force)
    override def whenTerminated(): Future[Unit] = connManager.whenTerminated()
    override def workers(): Future[Seq[WorkerLink]] = ???
    override def describe(): ClusterInfo = ???
  }

  class WorkerLaunchWrapper extends ClusterLauncher {

    override def launch(ctx: ContextConfig): FutureRef[ClusterStopper, Cluster] = {

    }
  }

  class AWSEMRLauncher extends ClusterLauncher {
    override def launch(ctx: ContextConfig): FutureRef[ClusterStopper, Cluster] = {

    }
  }
}


sealed trait DeployWorkerSettings
case class DirectWorkerLauncher(launcher: WorkerLauncher) extends DeployWorkerSettings
case class LaunchEMR(setupData: EMRProvisionerConfig) extends DeployWorkerSettings

trait Clusters {
  def startCluster(ctx: ContextConfig): Future[Cluster]
}

object Clusters {

  type Selector = ContextConfig => ClusterLauncher

  class ClustersHub(selector: Selector) extends Actor with ActorLogging {

    type State = Map[String, Cluster]
    type Reqs = Map[String, Promise[Cluster]]

    override def receive: Receive = process(Map.empty, Map.empty)

    import ClustersHub._

    private def process(
      clusters: State,
      requests: Reqs
    ): Receive = {
      case Event.StartCluster(ctx, req) =>
        selector(ctx) match {
          case DirectWorkerLauncher(launcher) =>
          case LaunchEMR(setupData) =>
        }

    }
  }

  object ClustersHub {
    sealed trait Event
    object Event {
      case class StartCluster(ctx: ContextConfig, req: Promise[Cluster]) extends Event
    }
  }

}

class ClustersHub(select: ContextConfig => DeployWorkerSettings) {


}

