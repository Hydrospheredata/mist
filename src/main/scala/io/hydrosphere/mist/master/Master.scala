package io.hydrosphere.mist.master

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.data.{EndpointsStorage, ContextsStorage}
import io.hydrosphere.mist.master.interfaces.async._
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.logging.{LogStorageMappings, LogStreams}
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.master.security.KInitLauncher

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls


/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {

    val appArguments = MasterAppArguments.parse(args) match {
      case Some(arg) => arg
      case None => sys.exit(1)
    }

    val config = MasterConfig.load(appArguments.configPath)

    if (config.security.enabled) {
      import scala.concurrent.ExecutionContext.Implicits.global
      import config.security._

      logger.info("Security is enabled - starting Knit loop")
      val ps = KInitLauncher.create(keytab, principal, interval)
      ps.run().onFailure({
        case e: Throwable =>
          logger.error("KInit process failed", e)
          sys.exit(1)
      })
    }

    val endpointsStorage = EndpointsStorage.create(config.endpointsPath, appArguments.routerConfigPath)
    val contextsStorage = ContextsStorage.create(config.contextsPath, appArguments.configPath)

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
    val jobsLogger = logsService.getLogger
    val statusService = system.actorOf(StatusService.props(store, eventPublishers, jobsLogger), "status-service")
    val infoProvider = new InfoProvider(config.logs, contextsStorage)
    val workerManager = system.actorOf(
      WorkersManager.props(
        statusService, workerRunner,
        jobsLogger,
        config.workers.runnerInitTimeout,
        infoProvider
      ), "workers-manager")

    val jobService = new JobService(workerManager, statusService)
    val masterService = new MasterService(
      jobService,
      endpointsStorage,
      contextsStorage,
      logsMappings
    )

    val precreated = Await.result(contextsStorage.precreated, Duration.Inf)
    precreated.foreach(context => {
      logger.info(s"Precreate context for ${context.name}")
      workerManager ! CreateContext(context)
    })

    // Start CLI
    system.actorOf(
      CliResponder.props(masterService, workerManager),
      name = Constants.Actors.cliResponderName)

    masterService.recoverJobs()

    val http = {
      val api = new HttpApi(masterService)
      val apiv2 = {
        val api = HttpV2Routes.apiWithCORS(masterService)
        val ws = new WSApi(streamer)
        // order is important!
        // api router can't chain unhandled calls, because it's wrapped in cors directive
        ws.route ~ api
      }
      val http = new HttpUi(config.http.uiPath).route ~ api.route ~ DevApi.devRoutes(masterService) ~ apiv2
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
