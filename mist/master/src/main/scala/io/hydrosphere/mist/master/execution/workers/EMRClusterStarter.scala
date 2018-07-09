package io.hydrosphere.mist.master.execution.workers

import akka.pattern.pipe
import akka.actor.{Actor, ActorLogging, ActorRef}
import io.hydrosphere.mist.master.{ClusterProvisionerConfig, EMRProvisionerConfig}
import io.hydrosphere.mist.master.execution.workers.EMRClusterStarter._
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event.AskConnection
import io.hydrosphere.mist.master.execution.workers.emr.{AwsEMRClient, StartClusterSettings}
import io.hydrosphere.mist.master.models.{AwsEMRConfig, ClusterConfig, ContextConfig}
import io.hydrosphere.mist.utils.Scheduling

import scala.concurrent.Future

class EMRClusterProvision(
  id: String,
  settings: StartClusterSettings,
  emrClient: AwsEMRClient,
  underlying: ActorRef
) extends Actor with ActorLogging {

  import context._

  def startCluster(): Future[]

  private def init: Receive = {
    def gotoAwaitStartResponse(asks: Vector[Event.AskConnection]): Unit = {
      log.info(s"Starting emr instance for $id")
      emrClient.start(settings).map(id => EmrStarting(id)) pipeTo self
      context become starting(asks)
    }

    def startCluster(): Unit = gotoAwaitStartResponse(Vector.empty)
    def startClusterWithAsk(ask: Event.AskConnection): Unit = gotoAwaitStartResponse(Vector(ask))

    {
      case Event.GetStatus => sender() ! SharedConnector.AwaitingRequest
      case ask :Event.AskConnection => startClusterWithAsk(ask)
      case Event.WarmUp => startCluster()
    }
  }

  private def

  private def starting(asks: Vector[AskConnection], id: String): Receive = {
    case ask: Event.AskConnection => context become starting(asks :+ ask)
    case EmrStarted(clusterId) =>
      log.info(s"Emr instance has been started: $clusterId")
      asks.foreach(a => underlying ! a)
      context become running(clusterId)

    case Event.Shutdown(force) =>
      log.warning("Non implemented normally")
      emrClient.stop()



  }

  private def running(clusterId: String): Receive = {

  }

  override def receive: Receive = init


}

object EMRClusterStarter {

  sealed trait EmrState
  case class EmrStarting(id: String) extends EmrState
  case class EmrStarted(id: String) extends EmrState
  case class EmrTerminated(e: Option[Throwable]) extends EmrState


  def mkStartSettings(
    name: String,
    provConfig: EMRProvisionerConfig,
    emrConfig: AwsEMRConfig
  ): StartClusterSettings = {
    import provConfig._
    import emrConfig._

    StartClusterSettings(
      name = name,
      keyPair = keyPair,
      releaseLabel = releaseLabel,
      masterInstanceType = masterInstanceType,
      slaveInstanceType = slaveInstanceType,
      instanceCount = instanceCount,
      subnetId = subnetId
    )
  }
}

