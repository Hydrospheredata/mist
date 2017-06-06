package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import io.hydrosphere.mist.Messages.StatusMessages.UpdateStatusEvent
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import interfaces.async._
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.interfaces.async.kafka.TopicProducer
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {

    val jobEndpoints = JobEndpoints.fromConfigFile(routerConfigPath())

    implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
    implicit val materializer = ActorMaterializer()

    val workerRunner = selectRunner(MistConfig.Workers.runner)
    val store = H2JobsRepository(MistConfig.History.filePath)

    val streamer = EventsStreamer(system)

    val wsPublisher = new JobEventPublisher {
      override def notify(event: UpdateStatusEvent): Unit =
        streamer.push(event)

      override def close(): Unit = {}
    }

    val eventPublishers = buildEventPublishers() :+ wsPublisher

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
      val apiv2Ws = new WSApi(streamer)
      val http = HttpUi.route ~ api.route ~ apiv2.route ~ apiv2Ws.route
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
      logger.error("Fatal error", e)
      sys.exit(1)
  }

  private def buildEventPublishers(): Seq[JobEventPublisher] = {
    val buffer = new ArrayBuffer[JobEventPublisher](3)
    if (MistConfig.Kafka.isOn) {
      import MistConfig.Kafka._


      buffer += JobEventPublisher.forKafka(host, port, publishTopic)
    }
    if (MistConfig.Mqtt.isOn) {
      import MistConfig.Mqtt._
      buffer += JobEventPublisher.forMqtt(host, port, publishTopic)
    }

    buffer
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
