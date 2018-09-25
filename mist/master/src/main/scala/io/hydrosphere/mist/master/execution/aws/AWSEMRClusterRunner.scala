package io.hydrosphere.mist.master.execution.aws

import java.nio.file.Path
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import cats.effect._
import cats.implicits._
import io.hydrosphere.mist.common.CommonData
import io.hydrosphere.mist.master.AWSEMRLaunchSettings
import io.hydrosphere.mist.master.execution.workers.{StopAction, WorkerConnection, WorkerRunner}
import io.hydrosphere.mist.master.execution.workers.starter.{SparkSubmitBuilder, WorkerProcess, WorkerStarter}
import io.hydrosphere.mist.master.execution.{Cluster, ClusterRunner, SpawnSettings}
import io.hydrosphere.mist.master.models.{AWSEMRLaunchData, ContextConfig}
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

object AWSEMRClusterRunner {

  def mkInstallCommands(
    agentJarPath: String,
    workerJarPath: String,
    agentId: String,
    masterAddr: String,
    accessKey: String,
    secretKey: String,
    region: String,
    awsId: String
  ): Seq[SSHCmd] = {
    Seq(
      SSHCmd.Exec(Seq("mkdir", "~/mist-agent")),
      SSHCmd.CopyFile(agentJarPath, "~/mist-agent/mist-agent.jar"),
      SSHCmd.CopyFile(workerJarPath, "~/mist-agent/mist-worker.jar"),
      SSHCmd.Exec(Seq(
        "java", "-cp", "~/mist-agent/mist-master.jar", "io.hydrosphere.mist.master.execution.aws.ClusterAgent",
        masterAddr, agentId, accessKey, secretKey, region, awsId,
        "1>~/mist-agent/out.log", "2>~/mist-agent/out.log", "&"
      ))
    )
  }

  class EMRClusterRunner(
    spawn: SpawnSettings,
    launchSettings: AWSEMRLaunchSettings,
    regHub: ActorRegHub,
    system: ActorSystem,
    installAgent: (String, String, String) => Unit,
    client: EMRClient[IO]
  ) extends ClusterRunner {

    implicit val IOTimer = new Timer[IO] {

      val ec = ExecutionContext.global
      val sc = Executors.newScheduledThreadPool(1)

      override val clock: Clock[IO] = new Clock[IO] {
          override def realTime(unit: TimeUnit): IO[Long] =
            IO(unit.convert(System.currentTimeMillis(), MILLISECONDS))

          override def monotonic(unit: TimeUnit): IO[Long] =
            IO(unit.convert(System.nanoTime(), NANOSECONDS))
        }

      override def sleep(timespan: FiniteDuration): IO[Unit] =
        IO.cancelable { cb =>
          val tick = new Runnable {
            def run() = ec.execute(new Runnable {
              def run() = cb(Right(()))
            })
          }
          val f = sc.schedule(tick, timespan.length, timespan.unit)
          IO(f.cancel(false))
        }
    }

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
        masterInstanceType = data.masterInstanceType,
        slaveInstanceType = data.slaveInstanceType,
        instanceCount = data.instanceCount,
        subnetId = settings.subnetId,
        additionalGroup = settings.additionalGroup,
        emrRole = settings.emrRole,
        emrEc2Role = settings.emrEc2Role
      )
    }

    private def startFully(runSettings: EMRRunSettings, client: EMRClient[IO]): IO[EmrInfo] = {
      for {
        initial <- client.start(runSettings)
        await <- client.awaitStatus(initial.id, EMRStatus.Started, 10 seconds, 40)
      } yield await
    }

    private def mkRunner(launch: AWSEMRLaunchSettings, host: String): WorkerRunner = {
      val starter = new WorkerStarter {
        val builder = new SparkSubmitBuilder("~/mist-agent", "/usr/lib/spark")
        override def onStart(name: String, initInfo: CommonData.WorkerInitInfo): WorkerProcess = {
          val submitCmd = builder.submitWorker(name, initInfo) :+ "&"
          Future {
            new SSHClient(host, launch.sshUser, launch.sshKeyPath).install(Seq(SSHCmd.Exec(submitCmd)))
          }
          WorkerProcess.NonLocal
        }

        override def stopAction: StopAction = StopAction.Remote
      }
      val hackSettings = spawn.copy(runnerCmd = starter)
      WorkerRunner.default(hackSettings, regHub, system)
    }

    override def run(id: String, ctx: ContextConfig): Future[Cluster] = {
      val io = for {
        data        <- extractData(ctx)
        runSettings =  mkRunSettings(id, data, launchSettings)
        emrInfo     <- startFully(runSettings, client)
        agentId     = s"agent-${emrInfo.id}"
        _           <- IO[Unit](installAgent(emrInfo.masterPublicDnsName, agentId, emrInfo.id))
        _           <- IO.fromFuture(IO(regHub.waitRef(agentId, 1 minute)))
        runner      = mkRunner(launchSettings, emrInfo.masterPublicDnsName)
        cluster     = Cluster.actorBased(id, ctx, runner, system)
      } yield cluster

      io.unsafeToFuture()
    }
  }

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
      val cmds = mkInstallCommands(
        jarsDir.resolve("mist-master.jar").toString, jarsDir.resolve("mist-worker.jar").toString,
        agentId: String, spawn.akkaAddress, accessKey, secretKey, region, awsId)
      new SSHClient(host, sshUser, sshKeyPath).install(cmds)
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
