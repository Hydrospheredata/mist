package io.hydrosphere.mist.master.execution.aws

import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import io.hydrosphere.mist.common.CommonData
import io.hydrosphere.mist.master.AWSEMRLaunchSettings
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, StopAction, WorkerConnection, WorkerRunner}
import io.hydrosphere.mist.master.execution.workers.starter.{SparkSubmitBuilder, WorkerProcess, WorkerStarter}
import io.hydrosphere.mist.master.execution.{Cluster, ClusterRunner, SpawnSettings}
import io.hydrosphere.mist.master.models.{AWSEMRLaunchData, ContextConfig, EMRInstance}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import IOTimer.Default

class EMRClusterRunner(
  spawn: SpawnSettings,
  launchSettings: AWSEMRLaunchSettings,
  regHub: ActorRegHub,
  system: ActorSystem,
  installAgent: (String, String, String) => Unit,
  client: EMRClient[IO]
) extends ClusterRunner {

  private def extractData(ctx: ContextConfig): IO[AWSEMRLaunchData] = {
    ctx.launchData match {
      case data: AWSEMRLaunchData => IO.pure(data)
      case other =>
        val err = new IllegalArgumentException(s"Invalid launch data for AWSCluster ${other.getClass.getSimpleName}")
        IO.raiseError(err)
    }
  }

  private def mkRunSettings(name: String, data: AWSEMRLaunchData, settings: AWSEMRLaunchSettings): EMRRunSettings = {
    EMRRunSettings(
      name = name,
      keyPair = settings.sshKeyPair,
      releaseLabel = data.releaseLabel,
      subnetId = settings.subnetId,
      additionalGroup = settings.additionalGroup,
      emrRole = settings.emrRole,
      emrEc2Role = settings.emrEc2Role,
      autoScalingRole = settings.autoScalingRole
    )
  }

  private def startFully(runSettings: EMRRunSettings, instances: Seq[EMRInstance.Instance], client: EMRClient[IO]): IO[EmrInfo] = {
    for {
      initial <- client.start(runSettings, instances)
      await <- client.awaitStatus(initial.id, EMRStatus.Started, 10 seconds, 40)
    } yield await
  }

  private def mkRunner(launch: AWSEMRLaunchSettings, host: String): WorkerRunner = {
    val starter: WorkerStarter = new WorkerStarter {
      val builder = new SparkSubmitBuilder(s"/home/${launch.sshUser}/mist-agent", "/usr/lib/spark")
      override def onStart(name: String, initInfo: CommonData.WorkerInitInfo): WorkerProcess = {
        val submitCmd = builder.submitWorker(name, initInfo) :+ "&"
        Future {
          new SSHClient(host, launch.sshUser, launch.sshKeyPath).install(Seq(SSHCmd.Exec(submitCmd)))
        }
        WorkerProcess.NonLocal
      }

      override def stopAction: StopAction = StopAction.Remote
    }

    WorkerRunner.default(spawn, starter, regHub, system)
  }

  override def run(id: String, ctx: ContextConfig): Future[Cluster] = {
    val io = for {
      data        <- extractData(ctx)
      runSettings =  mkRunSettings(id, data, launchSettings)
      emrInfo     <- startFully(runSettings, data.instances, client)
      agentId     =  s"agent-${emrInfo.id}"
      _           <- IO[Unit](installAgent(emrInfo.masterPublicDnsName, agentId, emrInfo.id))
      _           <- IO.fromFuture(IO(regHub.waitRef(agentId, 1 minute)))
      runner      =  mkRunner(launchSettings, emrInfo.masterPublicDnsName)
      cluster     =  Cluster.actorBased(id, ctx, runner, system)
      _           =  cluster.whenTerminated().onComplete(_ => client.stop(emrInfo.id))
    } yield cluster

    io.unsafeToFuture()
  }
}

object EMRClusterRunner extends Logger {

  def create(
    jarsDir: Path,
    spawn: SpawnSettings,
    launchSettings: AWSEMRLaunchSettings,
    regHub: ActorRegHub,
    system: ActorSystem
  ): ClusterRunner = {
    import launchSettings._
    val client = EMRClient.create(accessKey, secretKey, region)

    val installAgent = (host: String, agentId: String, awsId: String) => {
      val realDir = jarsDir.toAbsolutePath.toRealPath()
      val transfer = TransferParams(
        realDir.resolve("mist-agent.jar").toString,
        realDir.resolve("mist-worker.jar").toString,
        s"/home/$sshUser/mist-agent"
      )
      val agentRunParams = AgentRunParams(
        agentId, spawn.akkaAddress, accessKey, secretKey, region, awsId
      )
      val cmds = AgentInstall.sshCommands(transfer, agentRunParams)
      new SSHClient(host, sshUser, sshKeyPath).install(cmds)
      ()
    }

    new EMRClusterRunner(
      spawn,
      launchSettings,
      regHub,
      system,
      installAgent,
      client
    )
  }
}

