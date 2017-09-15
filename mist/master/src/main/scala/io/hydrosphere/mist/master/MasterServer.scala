package io.hydrosphere.mist.master

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.Messages.JobExecution.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.master.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.interfaces.async._
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.logging.{LogService, LogStorageMappings, LogStreams}
import io.hydrosphere.mist.master.security.KInitLauncher
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls

class MasterServer(
  config: MasterConfig,
  routerConfig: String
) extends Logger {

  private var security: Option[KInitLauncher.LoopedProcess] = None
  private var httpBinding: Option[ServerBinding] = None
  private var mqttInterface: Option[AsyncInterface] = None
  private var kafkaInterface: Option[AsyncInterface] = None
  private var workerManager: Option[ActorRef] = None
  private var eventPublishers: Seq[JobEventPublisher] = Seq()
  private var logsService: Option[LogService] = None
  private implicit val system = ActorSystem("mist", config.raw)
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = system.dispatcher

  def start(): Future[Unit] = Future {
    logger.info("Starting mist master")
    security = bootstrapSecurity

    val endpointsStorage = EndpointsStorage.create(config.endpointsPath, routerConfig)
    val contextsStorage = ContextsStorage.create(config.contextsPath, config.srcConfigPath)

    val workerRunner = WorkerRunner.create(config)

    val store = H2JobsRepository(config.dbPath)

    val streamer = EventsStreamer(system)

    val wsPublisher = new JobEventPublisher {
      override def notify(event: SystemEvent): Unit =
        streamer.push(event)

      override def close(): Unit = {}
    }

    eventPublishers = buildEventPublishers(config) :+ wsPublisher

    val logsMappings = LogStorageMappings.create(config.logs.dumpDirectory)
    val logsServiceVal = LogStreams.runService(
      config.logs.host, config.logs.port,
      logsMappings, eventPublishers
    )
    logsService = Some(logsServiceVal)
    val jobsLogger = logsServiceVal.getLogger
    val statusService = system.actorOf(StatusService.props(store, eventPublishers, jobsLogger), "status-service")
    val infoProvider = new InfoProvider(config.logs, config.http, contextsStorage, config.jobsSavePath)

    val workerManagerVal = system.actorOf(
      WorkersManager.props(
        statusService, workerRunner,
        jobsLogger,
        config.workers.runnerInitTimeout,
        infoProvider
      ), "workers-manager")

    workerManager = Some(workerManagerVal)

    val jobService = new JobService(workerManagerVal, statusService)

    val artifactRepository = ArtifactRepository.create(
      config.artifactRepositoryPath,
      endpointsStorage.defaults,
      config.jobsSavePath
    )
    val masterService = new MasterService(
      jobService,
      endpointsStorage,
      contextsStorage,
      logsMappings,
      artifactRepository
    )

    val precreated = Await.result(contextsStorage.precreated, Duration.Inf)
    precreated.foreach(context => {
      logger.info(s"Precreate context for ${context.name}")
      workerManagerVal ! CreateContext(context)
    })

    // Start CLI
    system.actorOf(
      CliResponder.props(masterService, workerManagerVal),
      name = CliResponder.Name)

    logger.info(s"${CliResponder.Name}: started")

    masterService.recoverJobs()

    httpBinding = Some(bootstrapHttp(streamer, masterService))

    // Start MQTT subscriber
    mqttInterface = bootstrapMqtt(masterService)

    // Start Kafka subscriber
    kafkaInterface = bootstrapKafka(masterService)
  }

  private def bootstrapKafka(masterService: MasterService): Option[AsyncInterface] = {
    if (config.kafka.isOn) {
      import config.kafka._

      val input = AsyncInput.forKafka(host, port, subscribeTopic)
      val interface = new AsyncInterface(masterService, input, Source.Async("Kafka")).start()
      logger.info("Kafka interface is started")
      Some(interface)
    } else {
      None
    }
  }

  private def bootstrapMqtt(masterService: MasterService): Option[AsyncInterface] = {
    if (config.mqtt.isOn) {
      import config.mqtt._

      val input = AsyncInput.forMqtt(host, port, subscribeTopic)
      val interface = new AsyncInterface(masterService, input, Source.Async("Mqtt")).start()
      logger.info("Mqtt interface is started")
      Some(interface)
    } else {
      None
    }
  }

  private def bootstrapHttp(streamer: EventsStreamer, masterService: MasterService): ServerBinding = {
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
    logger.info(s"REST started on ${config.http.host}:${config.http.port}")
    Await.result(http, Duration.Inf)
  }

  private def bootstrapSecurity: Option[KInitLauncher.LoopedProcess] = if (config.security.enabled) {
    import config.security._

    logger.info("Security is enabled - starting Knit loop")
    val ps = KInitLauncher.create(keytab, principal, interval)
    ps.run().onFailure({
      case e: Throwable =>
        logger.error("KInit process failed", e)
        stopUnexpectedly()
    })
    logger.info("Knit loop started")
    Some(ps)
  } else {
    None
  }

  def stopUnexpectedly(): Unit = {
    logger.info("Stop unexpectedly")
    sys.exit(1)
  }

  def stop(): Future[Unit] = {
    logger.info("Stopping mist master")
    for {
      _ <- {
        logger.info("Unbinding http port")
        optionSwitch(httpBinding.map(_.unbind()))
      }
      _ = mqttInterface.foreach(_.close())
      _ = kafkaInterface.foreach(_.close())
      _ <- {
        optionSwitch(security.map(_.stop()))
      }
      _ = workerManager.foreach(_ ! StopAllWorkers)
      _ = eventPublishers.foreach(_.close())
      _ <- {
        logger.info("Unbinding log service")
        optionSwitch(logsService.map(_.close()))
      }
      _ = system.shutdown()
    } yield ()
  }

  private def optionSwitch(of: Option[Future[Unit]]): Future[Option[Unit]] = of match {
    case Some(f) => f.map(Some.apply)
    case None => Future.successful(None)
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

object MasterServer {

  def apply(config: MasterConfig, routerConfig: String): MasterServer =
    new MasterServer(config, routerConfig)

  def apply(configPath: String, routerConfig: String): MasterServer = {
    val config = MasterConfig.load(configPath)
    apply(config, routerConfig)
  }
}