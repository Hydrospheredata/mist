package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master.{ClusterProvisionerConfig, EMRProvisionerConfig}
import io.hydrosphere.mist.master.execution.workers.emr.{AwsEMRClient, EMRRunSettings, EMRStatus}
import io.hydrosphere.mist.master.models.{AwsEMRConfig, ClusterConfig, ContextConfig, NoCluster}
import io.hydrosphere.mist.utils.{Logger, Scheduling}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

import scala.concurrent.ExecutionContext.Implicits.global

trait Provisioner {
  def provision(name: String, ctx: ContextConfig): Future[WorkerConnector]
}

class EMRProvisioner(
  config: EMRProvisionerConfig,
  client: AwsEMRClient
) extends Logger {

  private val scheduling = Scheduling.stpBased(2)

  private def awaitStarted(id: String, tick: FiniteDuration, tries: Int): Future[Unit] = {

    def processStatus(id: String, tick: FiniteDuration, tries: Int)(s: EMRStatus): Future[Unit] = s match {
      case EMRStatus.Starting =>
        if (tries <= 0) {
          client.stop(id).onComplete {
            case Success(_) => logger.info(s"Cluster $id was stopped")
            case Failure(e) => logger.warn(s"Cluster $id stopping was failed", e)
          }
          Future.failed(new RuntimeException("Starting cluster timeout exceeded"))
        } else {
          awaitStarted(id, tick, tries -1)
        }
      case EMRStatus.Started => Future.successful(())
      case EMRStatus.Terminated | EMRStatus.Terminating =>
        Future.failed(new RuntimeException(s"Cluster $id was terminated"))
    }

    scheduling.delay(tick)
      .flatMap(_ => client.status(id))
      .flatMap(s => processStatus(id, tick, tries)(s))
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

  def provision(name: String, emrSetup: AwsEMRConfig): Future[String] = {
    val startSettings = mkStartSettings(name, config, emrSetup)
    client.start(startSettings)
      .flatMap(id => awaitStarted(id, 5 seconds, 60).map(_ => id))
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
    default: (String, ContextConfig) => WorkerConnector
  ) extends Provisioner with Logger {

    override def provision(name: String, ctx: ContextConfig): Future[WorkerConnector] = {
      ctx.clusterConfig match {
        case NoCluster => Future.successful(default(name, ctx))
        case emrConfig: AwsEMRConfig =>
          emrs.get(emrConfig.provisionerId) match {
            case None => Future.failed(new IllegalArgumentException(s"Unknown provisioner: ${emrConfig.provisionerId}"))
            case Some(prov) =>
              prov.provision(name, emrConfig).map(id => {
                logger.info(s"Cluster for $name ctx has been started")
                throw new NotImplementedError("hehehe")
              })
          }
      }
    }
  }

  def create(
    settings: Seq[(String, ClusterProvisionerConfig)],
    default: (String, ContextConfig) => WorkerConnector
  ): Provisioner = {

    val emrs = settings.map({case (name, conf) =>
      val prov = conf match {
        case emrCfg: EMRProvisionerConfig => EMRProvisioner(emrCfg)
      }
      name -> prov
    }).toMap

    new DefaultProvisioner(emrs, default)
  }
}
