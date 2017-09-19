package io.hydrosphere.mist.master

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.Messages.WorkerMessages.{CreateContext, StopAllWorkers}
import io.hydrosphere.mist.jobs.JobDetails.Source
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

  def start(): Future[Unit] = {
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
    val infoProvider = new InfoProvider(config.logs, contextsStorage)

    for {
      logService       <- LogStreams.runService(
                            config.logs.host, config.logs.port,
                            logsMappings, eventPublishers)
      _                =  {
                             logger.info(s"Logger Service is started")
                             logsService = Some(logService)
                       }
      jobsLogger       =  logService.getLogger
      statusService    =  system.actorOf(StatusService.props(store, eventPublishers, jobsLogger), "status-service")
      workerManagerVal =  system.actorOf(
                            WorkersManager.props(
                              statusService, workerRunner,
                              jobsLogger,
                              config.workers.runnerInitTimeout,
                              infoProvider
                            ), "workers-manager")
      _                =  {
                             logger.info("Worker Manager is Started")
                             workerManager = Some(workerManagerVal)
                       }
      contexts         <- contextsStorage.precreated
      _                =  contexts.foreach(context => {
                            logger.info(s"Precreate context for ${context.name}")
                            workerManagerVal ! CreateContext(context)
                          })
      jobService       =  new JobService(workerManagerVal, statusService)
      masterService    =  new MasterService(jobService, endpointsStorage, contextsStorage, logsMappings)
      _                =  {
                            system.actorOf(
                              CliResponder.props(masterService, workerManagerVal),
                              name = Constants.Actors.cliResponderName)
                            // Start CLI
                            logger.info(s"${Constants.Actors.cliResponderName}: started")
                       }
      _                <- {
                            logger.info("Recovering jobs")
                            masterService.recoverJobs()
                       }
      binding          <- {
                            logger.info(s"Binding http ${config.http.host}:${config.http.port}")
                            bootstrapHttp(streamer, masterService)
                       }
      _                =  {
                            logger.info(s"Rest started on: $binding")
                            httpBinding = Some(binding)
                       }
      _                =  {
                            mqttInterface  = bootstrapMqtt(masterService)
                            kafkaInterface = bootstrapKafka(masterService)
                       }
    } yield ()
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

  private def bootstrapHttp(streamer: EventsStreamer, masterService: MasterService): Future[ServerBinding] = {
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
    http
  }

  private def bootstrapSecurity: Option[KInitLauncher.LoopedProcess] = if (config.security.enabled) {
    import config.security._

    logger.info("Security is enabled - starting Knit loop")
    val ps = KInitLauncher.create(keytab, principal, interval)
    ps.run().onFailure {
      case e: Throwable =>
        logger.error(s"KInit failed ${e.getMessage}, exiting...", e)
        sys.exit(1)
    }
    Some(ps)
  } else {
    None
  }

  def stop(): Future[Unit] = {
    logger.info("Stopping mist master")
    for {
      _ <- {
        httpBinding match {
          case Some(b) =>
            logger.info("Unbinding http port")
            b.unbind()
          case None =>
            Future.successful(())
        }
      }
      _ = {
        logger.info("Stopping mqtt")
        mqttInterface.foreach(_.close())
      }
      _ = {
        logger.info("Stopping kafka")
        kafkaInterface.foreach(_.close())
      }
      _ = {
        logger.info("Stopping workers")
        workerManager.foreach(_ ! StopAllWorkers)
      }
      _ = {
        logger.info("Stopping event publishers")
        eventPublishers.foreach(_.close())
      }
      _ <- security match {
        case Some(ps) =>
          logger.info("Stopping security")
          ps.stop()
        case None =>
          Future.successful(())
      }
      _ <- logsService match {
        case Some(s) =>
          logger.info("Unbinding log service")
          s.close()
        case None =>
          Future.successful(())
      }
      _ = system.shutdown()
    } yield ()
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