package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.Executors

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.{SpawnSettings, workers}
import io.hydrosphere.mist.master.{ClusterProvisionerConfig, EMRProvisionerConfig}
import io.hydrosphere.mist.master.execution.workers.emr.{AwsEMRClient, EMRRunSettings, EMRStatus}
import io.hydrosphere.mist.master.execution.workers.starter.{SSHRunner, WorkerStarter}
import io.hydrosphere.mist.master.models.{AwsEMRConfig, ClusterConfig, ContextConfig, NoCluster}
import io.hydrosphere.mist.utils.akka.ActorRegHub
import io.hydrosphere.mist.utils.{Logger, Scheduling}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

trait Provisioner {
  def provision(name: String, ctx: ContextConfig): Future[WorkerConnector]
}

case class EMRClusterInfo(
  publicDns: String,
  secGroup: String
)

class EMRProvisioner(
  config: EMRProvisionerConfig,
  client: AwsEMRClient
) extends Logger {

  private val scheduling = Scheduling.stpBased(2)

  private def awaitStarted(id: String, tick: FiniteDuration, tries: Int): Future[EMRClusterInfo] = {

    def processStatus(id: String, tick: FiniteDuration, tries: Int)(s: EMRStatus): Future[EMRClusterInfo] = s match {
      case EMRStatus.Starting =>
        if (tries <= 0) {
          client.stop(id).onComplete {
            case Success(_) => logger.info(s"Cluster $id was stopped")
            case Failure(e) => logger.warn(s"Cluster $id stopping was failed", e)
          }
          Future.failed(new RuntimeException("Starting cluster timeout exceeded"))
        } else {
          logger.info(s"Waiting cluster $id, tires left $tries")
          awaitStarted(id, tick, tries -1)
        }
      case EMRStatus.Started(addr, secGroup) => Future.successful(EMRClusterInfo(addr, secGroup))
      case EMRStatus.Terminated | EMRStatus.Terminating =>
        Future.failed(new RuntimeException(s"Cluster $id was terminated"))
    }

    scheduling.delay(tick)
      .flatMap(_ => client.status(id))
      .flatMap(s => processStatus(id, tick, tries)(s))
      .flatMap(i => client.allowIngress(config.privateIp + "/32", i.secGroup).map(_ => i))
  }

  private def mkStartSettings(
    name: String,
    provConfig: EMRProvisionerConfig,
    emrConfig: AwsEMRConfig
  ): EMRRunSettings = {

    import emrConfig._
    import provConfig._

    EMRRunSettings(
      name = name,
      keyPair = keyPair,
      releaseLabel = releaseLabel,
      masterInstanceType = masterInstanceType,
      slaveInstanceType = slaveInstanceType,
      instanceCount = instanceCount,
      subnetId = subnetId
    )
  }

  def provision(name: String, emrSetup: AwsEMRConfig): Future[EMRClusterInfo] = {
    val max = config.initTimeout match {
      case f: FiniteDuration => f
      case _ => 20 minutes
    }
    val maxTimes = (max / 5.seconds).floor.toInt
    val startSettings = mkStartSettings(name, config, emrSetup)
    client.start(startSettings)
      .flatMap(id => awaitStarted(id, 5 seconds, maxTimes))
  }


  def provision2(name: String, emrSetup: AwsEMRConfig): Future[WorkerStarter] = {
    val max = config.initTimeout match {
      case f: FiniteDuration => f
      case _ => 20 minutes
    }
    val maxTimes = (max / 5.seconds).floor.toInt
    val startSettings = mkStartSettings(name, config, emrSetup)
    client.start(startSettings)
      .flatMap(id => awaitStarted(id, 5 seconds, maxTimes))
      .map(info => {
        new SSHRunner("hadoop", config.localKeyPath, info.publicDns, ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor()))
      })

  }
}

object EMRProvisioner {

  def apply(cfg: EMRProvisionerConfig): EMRProvisioner = {
    val client = AwsEMRClient.create(cfg.accessKey, cfg.secretKey, cfg.region)
    new EMRProvisioner(cfg, client)
  }
}


object Provisioner {

  class DefaultProvisioner(
    emrs: Map[String, EMRProvisioner],

    spawnSettings: SpawnSettings,
    regHub: ActorRegHub,
    workerHub: WorkerHub,
    af: ActorRefFactory
  ) extends Provisioner with Logger {

    override def provision(name: String, ctx: ContextConfig): Future[WorkerConnector] = {

      def mkConnector(starter: WorkerStarter): WorkerConnector = {
        val connect = (id: String, info: WorkerInitInfo, ready: FiniteDuration, remote: ActorRef, stopAction: StopAction) => {
          WorkerBridge.connect(id, info, ready, remote, stopAction)(af)
        }
        val x = new workers.WorkerRunner.DefaultRunner(
          spawnSettings,
          starter,
          regHub,
          connect
        )
        val zz = new WorkerRunner {
          override def apply(v1: String, v2: ContextConfig): Future[WorkerConnection] = {
            val f = x(v1, v2)
            f.onComplete{
              case Success(conn) => workerHub.register(conn)
              case Failure(e) =>
                logger.error(s"Failed to start worker $v1 for ${v2.name}")

            }
            f
          }

        }

        WorkerConnector.actorBased(name, ctx, zz, af)
      }

      ctx.clusterConfig match {
        case NoCluster => Future.successful(mkConnector(spawnSettings.runnerCmd))
        case emrConfig: AwsEMRConfig =>
          emrs.get(emrConfig.provisionerId) match {
            case None => Future.failed(new IllegalArgumentException(s"Unknown provisioner: ${emrConfig.provisionerId}"))
            case Some(prov) =>
              prov.provision2(name, emrConfig).map(starter => {
                logger.info(s"Cluster for $name ctx has been started")
                mkConnector(starter)
              })
          }
      }
    }
  }

  def create(
    settings: Seq[(String, ClusterProvisionerConfig)],

    spawnSettings: SpawnSettings,
    regHub: ActorRegHub,
    workerHub: WorkerHub,
    af: ActorRefFactory
  ): Provisioner = {

    val emrs = settings.map({case (name, conf) =>
      val prov = conf match {
        case emrCfg: EMRProvisionerConfig => EMRProvisioner(emrCfg)
      }
      name -> prov
    }).toMap

    new DefaultProvisioner(emrs, spawnSettings, regHub, workerHub, af)
  }
}
