package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import interfaces.async._
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http.{WsEventPublisher, HttpApi, HttpApiV2, HttpUi}
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {
    implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
    implicit val materializer = ActorMaterializer()

    val jobEndpoints = JobEndpoints.fromConfigFile(MistConfig.Http.routerConfigPath)

    val workerRunner = selectRunner(MistConfig.Workers.runner)
    val store = H2JobsRepository(MistConfig.History.filePath)

    val eventPublishers = buildEventPublishers(system, materializer)

    val statusService = system.actorOf(StatusService.props(store, eventPublishers), "status-service")
    val workerManager = system.actorOf(WorkersManager.props(statusService, workerRunner), "workers-manager")

    val masterService = new MasterService(
      workerManager,
      statusService,
      jobEndpoints)

    MistConfig.Contexts.precreated foreach { name =>
      logger.info(s"Precreate context for $name namespace")
      workerManager ! CreateContext(name)
    }

    // Start CLI
    system.actorOf(
      CliResponder.props(masterService, workerManager),
      name = Constants.Actors.cliResponderName)


    //TODO: we should recover hobs before start listening on any interface
    //TODO: why we restart only async?
    //masterService.recoverJobs()

    if (MistConfig.Http.isOn) {
      val api = new HttpApi(masterService)
      val apiv2 = new HttpApiV2(masterService)
      val http = HttpUi.route ~ api.route ~ apiv2.route
      Http().bindAndHandle(http, MistConfig.Http.host, MistConfig.Http.port)
    }

    // Start MQTT subscriber
    if (MistConfig.Mqtt.isOn) {
      import MistConfig.Mqtt._

      val input = AsyncInput.forMqtt(host, port, subscribeTopic)
      new AsyncInterface(masterService, input, Source.Async("Mqtt")).start()
      logger.info("Mqtt interface is started")
    }

    // Start Kafka subscriber
    if (MistConfig.Kafka.isOn) {
      import MistConfig.Kafka._

      val input = AsyncInput.forKafka(host, port, subscribeTopic)
      new AsyncInterface(masterService, input, Source.Async("Kafka")).start()
      logger.info("Kafka interface is started")
    }



    // We need to stop contexts on exit
    sys addShutdownHook {
      logger.info("Stopping all the contexts")
      workerManager ! StopAllWorkers
      system.shutdown()
    }
  } catch {
    case e: Throwable =>
      logger.error("Service is crashed", e)
      sys.exit(3)
  }

  private def buildEventPublishers(sys: ActorSystem, materializer: ActorMaterializer): Seq[JobEventPublisher] = {
    val buffer = new ArrayBuffer[JobEventPublisher](3)
    if (MistConfig.Kafka.isOn) {
      import MistConfig.Kafka._
      buffer += JobEventPublisher.forKafka(host, port, publishTopic)
    }
    if (MistConfig.Mqtt.isOn) {
      import MistConfig.Mqtt._
      buffer += JobEventPublisher.forMqtt(host, port, publishTopic)
    }

    buffer += WsEventPublisher.blabla(materializer)

    buffer
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
