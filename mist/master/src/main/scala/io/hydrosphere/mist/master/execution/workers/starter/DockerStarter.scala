package io.hydrosphere.mist.master.execution.workers.starter

import java.util.concurrent.Executors

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.{CreateContainerCmd, CreateVolumeCmd, InspectContainerResponse}
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.DockerRunnerConfig
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.util._

sealed trait WorkerImageConfiguration
case object DoNothing extends WorkerImageConfiguration
case class CloneMasterConfig(masterId: String) extends WorkerImageConfiguration

class DockerStarter(
  builder: SparkSubmitBuilder,
  dockerClient: DockerClient,
  configuration: WorkerImageConfiguration,
  image: String
) extends WorkerStarter with Logger {

  private implicit val ec = {
    val service = Executors.newFixedThreadPool(1)
    ExecutionContext.fromExecutorService(
      service,
      e => logger.error("Error from docker ec", e)
    )
  }

  def copyMasterSettings(id: String, cmd: CreateContainerCmd): Future[CreateContainerCmd] = {
    def asJList[A](a: Array[A]): java.util.List[A] = java.util.Arrays.asList(a: _*)

    def cloneLinks(inspect: InspectContainerResponse, cmd: CreateContainerCmd): CreateContainerCmd = {
      val networks = inspect.getNetworkSettings.getNetworks.asScala
      if (networks.size > 1) {
        val ids = networks.keys.mkString(",")
        logger.warn(s"Detected several networks for $id : $ids")
      }
      val network = networks.head._2
      val links = network.getLinks
      if (links.nonEmpty) cmd.withLinks(asJList(links)) else cmd
    }

    for {
      master <- Future { dockerClient.inspectContainerCmd(id).exec() }
      patched = cloneLinks(master, cmd)
    } yield patched

  }

  private def runContainer(name: String, init: WorkerInitInfo): Future[String] = {
    def patchCommand(cmd: CreateContainerCmd): Future[CreateContainerCmd] = configuration match {
      case DoNothing => Future.successful(cmd)
      case CloneMasterConfig(id) => copyMasterSettings(id, cmd)
    }

    val initial = dockerClient.createContainerCmd(image)
      .withEntrypoint(builder.submitWorker(name, init).asJava)

    for {
      cmd  <- patchCommand(initial)
      resp <- Future { cmd.exec() }
      _    <- Future { dockerClient.startContainerCmd(resp.getId).exec() }
    } yield resp.getId
  }

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    runContainer(name, initInfo).onComplete({
      case Success(id) => logger.info(s"Container $id started for worker $name")
      case Failure(e) => logger.error(s"Container starting for worker $name failed", e)
    })
    NonLocal
  }

}

object DockerStarter {

  def apply(config: DockerRunnerConfig): DockerStarter = {
    import config._

    val imageConf = masterContainerId match {
      case Some(id) => CloneMasterConfig(id)
      case None => DoNothing
    }

    val client = {
      val config = DefaultDockerClientConfig.createDefaultConfigBuilder()
        .withDockerHost(dockerHost)
        .build()

      DockerClientBuilder.getInstance(config).build()
    }

    new DockerStarter(
      new SparkSubmitBuilder(mistHome, sparkHome),
      client,
      imageConf,
      image
    )
  }
}

