package io.hydrosphere.mist.master

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http.{HttpApi, HttpUi}
import io.hydrosphere.mist.master.store.JobRepository
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {
    val jobRoutes = new JobRoutes(routerConfigPath())

    implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
    implicit val materializer = ActorMaterializer()

    val workerRunner = selectRunner(MistConfig.Workers.runner)
    val store = JobRepository()
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
      val http = HttpUi.route ~ api.route
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

    val publishers = enabledAsyncPublishers()
    masterService.recoverJobs(publishers)

    // We need to stop contexts on exit
    sys addShutdownHook {
      logger.info("Stopping all the contexts")
      workerManager ! StopAllWorkers
      system.shutdown()
    }
  } catch {
    case e: Throwable =>
      logger.error("Fatal error", e)
      sys.exit(1)
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
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(home) => new LocalWorkerRunner(home)
        }
      case "docker" => DockerWorkerRunner
      case "manual" => ManualWorkerRunner
      case _ =>
        throw new IllegalArgumentException(s"Unknown worker runner type $s")

    }
  }

  private def routerConfigPath(): String = {
    if (args.length > 0)
      args(0)
    else
      "configs/router.conf"
  }
}


