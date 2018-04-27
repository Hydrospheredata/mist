package io.hydrosphere.mist.master.execution.workers.starter

import java.util.concurrent.Executors

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.{CreateContainerCmd, InspectContainerResponse}
import com.github.dockerjava.api.model.{ContainerNetwork, Link}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.execution.workers.StopAction
import io.hydrosphere.mist.utils.Logger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util._

class DockerStarter(
  builder: SparkSubmitBuilder,
  dockerClient: DockerClient,
  networkConf: DockerNetworkConfiguration,
  image: String
) extends WorkerStarter with Logger {

  private implicit val ec = {
    val service = Executors.newFixedThreadPool(1)
    ExecutionContext.fromExecutorService(
      service,
      e => logger.error("Error from docker ec", e)
    )
  }

  def autoConfigureNetwork(id: String, cmd: CreateContainerCmd): Future[CreateContainerCmd] = {
    def asJList[A](a: Array[A]): java.util.List[A] = java.util.Arrays.asList(a: _*)

    def setupBridge(masterName: String, network: ContainerNetwork, cmd: CreateContainerCmd): CreateContainerCmd = {
      val links = network.getLinks
      val copied = if (links.nonEmpty) cmd.withLinks(asJList(links)) else cmd
      val masterLink = new Link(masterName, "master")
      copied.withLinks(List(masterLink).asJava).withNetworkMode("bridge")
    }

    def setup(networkName: String, network: ContainerNetwork, cmd: CreateContainerCmd): CreateContainerCmd = {
      cmd.withAliases(network.getAliases).withNetworkMode(networkName)
    }

    def configure(master: InspectContainerResponse, cmd: CreateContainerCmd): CreateContainerCmd = {
      val networks = master.getNetworkSettings.getNetworks.asScala
      if (networks.size > 1) {
        val ids = networks.keys.mkString(",")
        logger.warn(s"Detected several networks for $id : $ids")
      }
      networks.head match {
        case ("bridge", network) => setupBridge(master.getName, network, cmd)
        case ("host", network) => cmd.withNetworkMode("host")
        case (unknown, network) => setup(unknown, network, cmd)
      }
    }

    for {
      master <- Future { dockerClient.inspectContainerCmd(id).exec() }
      patched = configure(master, cmd)
    } yield patched

  }

  private def runContainer(name: String, init: WorkerInitInfo): Future[String] = {
    def setupNetworking(cmd: CreateContainerCmd): Future[CreateContainerCmd] = networkConf match {
      case NamedNetwork(name) => Future.successful(cmd.withNetworkMode(name))
      case AutoMasterNetwork(id) => autoConfigureNetwork(id, cmd)
    }

    val initial = dockerClient.createContainerCmd(image)
      .withEntrypoint(builder.submitWorker(name, init).asJava)
      .withLabels(Map("mist.worker" -> name).asJava)

    for {
      cmd  <- setupNetworking(initial)
      resp <- Future { cmd.exec() }
      _    <- Future { dockerClient.startContainerCmd(resp.getId).exec() }
    } yield resp.getId
  }

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    runContainer(name, initInfo).onComplete({
      case Success(id) => logger.info(s"Container $id started for worker $name")
      case Failure(e) => logger.error(s"Container starting for worker $name failed", e)
    })
    WorkerProcess.NonLocal
  }

  override def stopAction: StopAction = StopAction.Remote
}

object DockerStarter {

  def apply(config: DockerRunnerConfig): DockerStarter = {
    import config._

    val client = {
      val config = DefaultDockerClientConfig.createDefaultConfigBuilder()
        .withDockerHost(dockerHost)
        .build()

      DockerClientBuilder.getInstance(config).build()
    }

    new DockerStarter(
      new SparkSubmitBuilder(mistHome, sparkHome),
      client,
      network,
      image
    )
  }
}

