package io.hydrosphere.mist.master.execution

import akka.pattern.pipe
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import io.hydrosphere.mist.master.EMRProvisionerConfig
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection, WorkerConnector, WorkerRunner}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.{Future, Promise}

case class ClusterRef(
  starter: ActorRef,
  future: Future[ActorRef]
)

sealed trait ClusterStatus
object ClusterStatus {
  final case class Working(initTime: Long, startTime: Long, ref: ActorRef) extends ClusterStatus
  final case class Starting(initTime: Long, ref: ClusterRef) extends ClusterStatus
}

trait ClusterLauncher {
  def run(ctx: ContextConfig): ClusterRef
}

case class MasterSettings(
  akkaAddress: String,
  logAddress: String,
  httpAddress: String,
  maxArtifactSize: Long
)

trait ClusterHub {

  def start(ctx: ContextConfig): ClusterRef

  def clusters(): Future[Seq[String]]
  def cluster(id: String): Future[String]

}



object ClusterHub {

  def apply(
    masterSettings: MasterSettings,
    system: ActorSystem
  ): ClusterHub = {


    
    new ClusterHub {

      override def start(ctx: ContextConfig): ClusterRef = ???

      override def cluster(id: String): Future[String] = ???

      override def clusters(): Future[Seq[String]] = ???
    }

  }

}

object ClusterLauncher {

  def apply(system: ActorSystem): ClusterLauncher = new ClusterLauncher()

}

//class ClustersHub(
//  clusterRunner: (ContextConfig, Promise[WorkerConnector]) => ClusterRef
//) extends Actor with ActorLogging {
//
//  import ClustersHub._
//  import context._
//
//  type State = Map[String, ClusterStatus]
//
//  override def receive: Receive = process(Map.empty)
//
//  private def process(state: State): Receive = {
//    case Event.StartCluster(ctx) =>
//      val clusterRef = clusterRunner(ctx)
//      val time = System.currentTimeMillis()
//      clusterRef.future pipeTo self
//      val next = state + (ctx.name -> ClusterStatus.Starting(time, clusterRef))
//      context become process(next)
//
//    case Event.ClusterStarted()
//  }
//
//}
//
//object ClustersHub {
//
//  sealed trait Event
//  object Event {
//    final case class StartCluster(ctx: ContextConfig, promise: Promise[ActorRef]) extends Event
//    final case class ClusterStarted(ctx)
//  }
//}
