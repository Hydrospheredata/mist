package io.hydrosphere.mist.master

import java.io.File

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http.{HttpApi, HttpApiV2, HttpUi}
import io.hydrosphere.mist.master.store.{H2JobsRepository, JobRepository}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
  implicit val materializer = ActorMaterializer()

  val jobRoutes = new JobRoutes(MistConfig.Http.routerConfigPath)

  val workerRunner = selectRunner(MistConfig.Workers.runner)
  val store = H2JobsRepository(MistConfig.History.filePath)
  val statusService = system.actorOf(StatusService.props(store), "status-service")
  val workerManager = system.actorOf(WorkersManager.props(statusService, workerRunner), "workers-manager")

  val masterService = new MasterService(
    workerManager,
    statusService,
    jobRoutes)

  MistConfig.Contexts.precreated foreach { name =>
    logger.info(s"Precreate context for $name namespace")
    workerManager ! CreateContext(name)
  }

  if (MistConfig.Http.isOn) {
    val api = new HttpApi(masterService)
    val apiv2 = new HttpApiV2(masterService)
    val http = HttpUi.route ~ api.route ~ apiv2.route
    Http().bindAndHandle(http, MistConfig.Http.host, MistConfig.Http.port)
  }

  // Start CLI
  system.actorOf(
    CliResponder.props(masterService, workerManager),
    name = Constants.Actors.cliResponderName)

  AsyncInterface.init(system, masterService)

  // Start MQTT subscriber
  if (MistConfig.Mqtt.isOn) {
    logger.info("Mqtt interface is started")
    AsyncInterface.subscriber(AsyncInterface.Provider.Mqtt)
  }

  // Start Kafka subscriber
  if (MistConfig.Kafka.isOn) {
    AsyncInterface.subscriber(AsyncInterface.Provider.Kafka)
  }

  //TODO: we should recover hobs before start listening on any interface
  //TODO: why we restart only async?
  val publishers = enabledAsyncPublishers()
  masterService.recoverJobs(publishers)

  // We need to stop contexts on exit
  sys addShutdownHook {
    logger.info("Stopping all the contexts")
    workerManager ! StopAllWorkers
    system.shutdown()
  }

  private def enabledAsyncPublishers(): Map[Provider, ActorRef] = {
    val providers = Seq[Provider](Provider.Kafka, Provider.Mqtt).filter({
      case Provider.Kafka => MistConfig.Kafka.isOn
      case Provider.Mqtt => MistConfig.Mqtt.isOn
    })

    providers.map(p => p -> AsyncInterface.publisher(p)).toMap
  }

  private def selectRunner(s: String): WorkerRunner = {
    s match {
      case "local" =>
        val sparkHome = System.getenv("SPARK_HOME").ensuring(_.nonEmpty, "SPARK_HOME is not defined!")
        new LocalWorkerRunner(sparkHome)

      case "docker" => DockerWorkerRunner
      case "manual" => ManualWorkerRunner
      case _ =>
        throw new IllegalArgumentException(s"Unknown worker runner type $s")

    }
  }
}
