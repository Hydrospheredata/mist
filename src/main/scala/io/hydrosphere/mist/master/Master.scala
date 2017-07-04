package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.interfaces.async._
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.logging.{LogStorageMappings, LogStreams}
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MasterConfig}

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls


/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {

    val appArguments = MasterAppArguments.parse(args) match {
      case Some(arg) => arg
      case None => sys.exit(1)
    }

    val config = MasterConfig.load(appArguments.configPath)

    val jobEndpoints = JobEndpoints.fromConfigFile(appArguments.routerConfigPath)

    implicit val system = ActorSystem("mist", config.raw)
    implicit val materializer = ActorMaterializer()

    val workerRunner = WorkerRunner.create(config)

    val store = H2JobsRepository(config.dbPath)

    val streamer = EventsStreamer(system)

    val wsPublisher = new JobEventPublisher {
      override def notify(event: SystemEvent): Unit =
        streamer.push(event)

      override def close(): Unit = {}
    }

    val eventPublishers = buildEventPublishers(config) :+ wsPublisher

    val logsMappings = LogStorageMappings.create(config.logs.dumpDirectory)
    val logsService = LogStreams.runService(
      config.logs.host, config.logs.port,
      logsMappings, eventPublishers
    )

    val statusService = system.actorOf(StatusService.props(store, eventPublishers), "status-service")
    val workerManager = system.actorOf(
      WorkersManager.props(statusService, workerRunner, logsService.getLogger), "workers-manager")

    val masterService = new MasterService(
      workerManager,
      statusService,
      jobEndpoints)

    config.contextsSettings.precreated.foreach(context => {
      val name = context.name
      logger.info(s"Precreate context for $name namespace")
      workerManager ! CreateContext(name)
    })

    // Start CLI
    system.actorOf(
      CliResponder.props(masterService, workerManager),
      name = Constants.Actors.cliResponderName)

    masterService.recoverJobs()

    val http = {
      val api = new HttpApi(masterService)
      val apiv2 = {
        val api = new HttpApiV2(masterService, logsMappings)
        val ws = new WSApi(streamer)
        CorsDirective.cors() { api.route ~ ws.route }
      }
      val http = HttpUi.route ~ api.route ~ apiv2
      Http().bindAndHandle(http, config.http.host, config.http.port)
    }

    // Start MQTT subscriber
    if (config.mqtt.isOn) {
      import config.mqtt._

      val input = AsyncInput.forMqtt(host, port, subscribeTopic)
      new AsyncInterface(masterService, input, Source.Async("Mqtt")).start()
      logger.info("Mqtt interface is started")
    }

    // Start Kafka subscriber
    if (config.kafka.isOn) {
      import config.kafka._

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
      logger.error("Fatal error", e)
      sys.exit(1)
  }

  private def buildEventPublishers(config: MasterConfig): Seq[JobEventPublisher] = {
    val buffer = new ArrayBuffer[JobEventPublisher](3)
    if (config.kafka.isOn) {
      import config.kafka._

      buffer += JobEventPublisher.forKafka(host, port, publishTopic)
    }
    if (config.mqtt.isOn) {
      import config.mqtt._
      buffer += JobEventPublisher.forMqtt(host, port, publishTopic)
    }

    buffer
  }

}
