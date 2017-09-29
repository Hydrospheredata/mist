package io.hydrosphere.mist.master

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.interfaces.async._
import io.hydrosphere.mist.master.interfaces.cli.CliResponder
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.logging.{JobsLogger, LogService, LogStoragePaths, LogStreams}
import io.hydrosphere.mist.master.security.KInitLauncher
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.util._

case class Step[A](name: String, f: () => Future[A]) {

  def exec(): Future[A] = f()

}

object Step {

  def future[A](name: String, f: => Future[A]): Step[A] = Step(name, () => f)

  def lift[A](name: String, f: => A): Step[A] = {
    val lifted = () => Try(f) match {
      case Success(a) => Future.successful(a)
      case Failure(e) => Future.failed(e)
    }
    Step(name, lifted)
  }

}

case class ServerInstance(closeSteps: Seq[Step[Unit]])
  extends Logger{

  def stop(): Future[Unit] = {
    def execStep(step: Step[Unit]): Future[Unit] = {
      val f = step.exec()
      f.onComplete {
        case Success(_) => logger.info(s"${step.name} closed")
        case Failure(e) => logger.error(s"Closing ${step.name} failed", e)
      }
      f
    }
    closeSteps.foldLeft(Future.successful(())) { (acc, step) => {
      acc.andThen({ case _ => execStep(step) })
    }}
  }
}

object MasterServer extends Logger {

  def start(configPath: String, routerPath: String): Future[ServerInstance] =
    start(MasterConfig.load(configPath), routerPath)

  def start(config: MasterConfig, routerConfig: String): Future[ServerInstance] = {
    implicit val system = ActorSystem("mist", config.raw)
    implicit val materializer = ActorMaterializer()

    val endpointsStorage = EndpointsStorage.create(config.endpointsPath, routerConfig)
    val contextsStorage = ContextsStorage.create(config.contextsPath, config.srcConfigPath)
    val store = H2JobsRepository(config.dbPath)

    val logsPaths = LogStoragePaths.create(config.logs.dumpDirectory)

    val streamer = runEventStreamer(config)

    def runLogService(): Future[LogService] = {
      import config.logs._
      LogStreams.runService(host, port, logsPaths, streamer)
    }

    def runJobService(jobsLogger: JobsLogger): JobService = {
      val workerRunner = WorkerRunner.create(config)
      val infoProvider = new InfoProvider(config.logs, contextsStorage)

      val status = system.actorOf(StatusService.props(store, streamer, jobsLogger), "status-service")
      val workerManager = system.actorOf(
        WorkersManager.props(
          status, workerRunner,
          jobsLogger,
          config.workers.runnerInitTimeout,
          infoProvider
        ), "workers-manager")

      new JobService(workerManager, status)
    }

    val security = bootstrapSecurity(config)

    for {
      logService      <- start("Jobs logging", runLogService())
      jobsService     =  runJobService(logService.getLogger)
      masterService   <- start("Main service", MainService.start(jobsService, endpointsStorage, contextsStorage, logsPaths))
      _               =  runCliInterface(masterService)
      httpBinding     <- start("Http interface", bootstrapHttp(streamer, masterService, config.http))
      asyncInterfaces =  bootstrapAsyncInput(masterService, config)

    } yield ServerInstance(
      Seq(Step.future("Http", httpBinding.unbind())) ++
      asyncInterfaces.map(i => Step.lift(s"Async interface: ${i.name}", i.close())) ++
      security.map(ps => Step.future("Security", ps.stop())) :+
      Step.future("System", {
        materializer.shutdown()
        system.shutdown()
        Future(system.awaitTermination())
      })
    )

  }

  private def bootstrapSecurity(config: MasterConfig): Option[KInitLauncher.LoopedProcess] = {
    config.security.map(cfg => {
      import cfg._
      logger.info("Security is enabled - starting Knit loop")
      val ps = KInitLauncher.create(keytab, principal, interval)
      ps.run().onFailure {
        case e: Throwable =>
          logger.error(s"KInit failed ${e.getMessage}, exiting...", e)
          sys.exit(1)
      }
      ps
    })
  }

  private def bootstrapHttp(
    streamer: EventsStreamer,
    mainService: MainService,
    config: HttpConfig)(implicit sys: ActorSystem, mat: ActorMaterializer): Future[ServerBinding] = {
    val http = {
      val api = new HttpApi(mainService)
      val apiv2 = {
        val api = HttpV2Routes.apiWithCORS(mainService)
        val ws = new WSApi(streamer)
        // order is important!
        // api router can't chain unhandled calls, because it's wrapped in cors directive
        ws.route ~ api
      }
      val http = new HttpUi(config.uiPath).route ~ api.route ~ DevApi.devRoutes(mainService) ~ apiv2
      Http().bindAndHandle(http, config.host, config.port)
    }
    http
  }

  private def bootstrapAsyncInput(mainService: MainService, config: MasterConfig): Seq[AsyncInterface] = {
    def startInterface(input: AsyncInput, name: String): AsyncInterface = {
      val interface = new AsyncInterface(mainService, input, name)
      interface.start()
      logger.info(s"Async interface: $name started")
      interface
    }

    val mqtt = config.mqtt.map(cfg => {
      import cfg._
      AsyncInput.forMqtt(host, port, subscribeTopic)
    }).map(startInterface(_, "Mqtt"))

    val kafka = config.kafka.map(cfg => {
      import cfg._
      AsyncInput.forKafka(host, port, subscribeTopic)
    }).map(startInterface(_, "Kafka"))

    Seq(mqtt, kafka).flatten
  }

  private def runCliInterface(masterService: MainService)
    (implicit sys: ActorSystem): ActorRef = {
    sys.actorOf(
      CliResponder.props(masterService, masterService.jobService.workerManager),
      name = Constants.Actors.cliResponderName)
  }


  private def runEventStreamer(config: MasterConfig)
    (implicit sys: ActorSystem, mat: ActorMaterializer): EventsStreamer = {
    val streamer = EventsStreamer(sys)

    def startStream(f: SystemEvent => Unit, name: String, close: () => Unit): Unit = {
      val complete = Sink.onComplete[Unit]({
        case Success(_) => logger.info(s"Event streaming for $name stopped")
        case Failure(e) => logger.error(s"Event streaming for $name was completed with error", e)
      })
      streamer.eventsSource().map(f).to(complete).run()
      logger.info(s"Event streaming for $name started")
    }

    config.kafka.foreach(cfg => {
      import cfg._
      val publisher = JobEventPublisher.forKafka(host, port, publishTopic)
      startStream(publisher.notify, s"kafka($host:$port/$publishTopic)", publisher.close)
    })

    config.mqtt.foreach(cfg => {
      import cfg._
      val publisher = JobEventPublisher.forMqtt(host, port, publishTopic)
      startStream(publisher.notify, s"mqtt($host:$port/$publishTopic", publisher.close)
    })

    streamer
  }

  private def start[A](name: String, f: => Future[A]): Future[A] = {
    val future = f
    future.onComplete {
      case Success(_) => logger.info(s"$name started")
      case Failure(e) => logger.error(s"Starting $name failed")
    }
    future
  }

  private def start[A](name: String, f: => A): A = {
    try {
      val value = f
      logger.info(s"$name started")
      value
    } catch {
      case e: Throwable =>
        logger.error(s"Starting $name failed", e)
        throw e
    }
  }
}
