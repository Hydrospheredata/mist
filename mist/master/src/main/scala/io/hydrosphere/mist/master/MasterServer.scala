package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.server.Directives._
import akka.pattern.gracefulStop
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision}
import akka.stream.scaladsl.{Keep, Sink}
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.master.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, FunctionConfigStorage}
import io.hydrosphere.mist.master.execution.workers.{RunnerCmd, RunnerCommand2}
import io.hydrosphere.mist.master.execution.{ExecutionService, SpawnSettings}
import io.hydrosphere.mist.master.interfaces.async._
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.jobs.{FunctionInfoProviderRunner, FunctionInfoService}
import io.hydrosphere.mist.master.logging.{LogService, LogStreams}
import io.hydrosphere.mist.master.security.KInitLauncher
import io.hydrosphere.mist.master.store.H2JobsRepository
import io.hydrosphere.mist.utils.{Logger, NetUtils}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
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

case class ServerInstance(closeSteps: Seq[Step[_]])
  extends Logger{

  def stop(): Future[Unit] = {
    def execStep(step: Step[_]): Future[Unit] = {
      val p = Promise[Unit]
      step.exec().onComplete {
        case Success(_) =>
          logger.info(s"${step.name} closed")
          p.success(())
        case Failure(e) =>
          logger.error(s"Closing ${step.name} failed", e)
          p.success(())
      }
      p.future
    }
    closeSteps.foldLeft(Future.successful()) {
      case (acc, step) => acc.flatMap(_ => execStep(step))
    }
  }
}

object MasterServer extends Logger {

  def start(configPath: String, routerPath: String): Future[ServerInstance] =
    start(MasterConfig.load(configPath), routerPath)

  def start(config: MasterConfig, routerConfig: String): Future[ServerInstance] = {
    implicit val system = ActorSystem("mist", config.raw)
    implicit val materializer = ActorMaterializer()

    // use an actor reference unexpected master shutdown or connection problems
    val healthRef = system.actorOf(Props(new Actor {
      override def receive: Receive = { case _ => }
    }), CommonData.HealthActorName)

    val functionsStorage = FunctionConfigStorage.create(config.functionsPath, routerConfig)
    val contextsStorage = ContextsStorage.create(config.contextsPath, config.srcConfigPath)
    val store = H2JobsRepository(config.dbPath)

    val logsPaths = LogStoragePaths.create(config.logs.dumpDirectory)

    val streamer = runEventStreamer(config)

    def runLogService(): Future[LogService] = {
      import config.logs._
      LogStreams.runService(host, port, logsPaths, streamer)
    }

    def runExecutionService(logService: LogService): ExecutionService = {
      val masterService = s"${config.cluster.host}:${config.cluster.port}"
      val workerRunner = RunnerCommand2.create(masterService, config.workers)
      val spawnSettings = SpawnSettings(
        runnerCmd = workerRunner,
        timeout = config.workers.runnerInitTimeout,
        readyTimeout = config.workers.readyTimeout,
        akkaAddress = masterService,
        logAddress = s"${config.logs.host}:${config.logs.port}",
        httpAddress = s"${config.http.host}:${config.http.port}",
        maxArtifactSize = config.workers.maxArtifactSize
      )
      ExecutionService(spawnSettings, system, streamer, store, logService)
    }

    val artifactRepository = ArtifactRepository.create(
      config.artifactRepositoryPath,
      functionsStorage.defaults
    )


    val security = bootstrapSecurity(config)

    val jobExtractorRunner = FunctionInfoProviderRunner.create(
      config.jobInfoProviderConfig,
      config.cluster.host,
      config.cluster.port
    )

    for {
      logService             <- start("LogsSystem", runLogService())
      jobInfoProvider        <- start("FunctionInfoProvider", jobExtractorRunner.run())
      jobInfoProviderService =  new FunctionInfoService(
                                      jobInfoProvider,
                                      functionsStorage,
                                      artifactRepository
                                )(system.dispatcher)
      executionService       =  runExecutionService(logService)
      masterService          <- start("Main service", MainService.start(
                                                        executionService,
                                                        functionsStorage,
                                                        contextsStorage,
                                                        logsPaths,
                                                        jobInfoProviderService
                                                     )
                             )
      httpBinding            <- start("Http interface", bootstrapHttp(streamer, masterService, artifactRepository, config.http))
      asyncInterfaces        =  bootstrapAsyncInput(masterService, config)

    } yield ServerInstance(
      Seq(Step.future("Http", httpBinding.unbind())) ++
      asyncInterfaces.map(i => Step.lift(s"Async interface: ${i.name}", i.close())) ++
      security.map(ps => Step.future("Security", ps.stop())) :+
      Step.lift("FunctionInfoProvider", healthRef ! PoisonPill) :+
      Step.future("LogsSystem", logService.close()) :+
      Step.future("System", {
        materializer.shutdown()
        system.terminate().map(_ => ())
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
    artifacts: ArtifactRepository,
    config: HttpConfig)(implicit sys: ActorSystem, mat: ActorMaterializer): Future[ServerBinding] = {
    val http = {
      val apiv2 = {
        val api = HttpV2Routes.apiWithCORS(mainService, artifacts, _root_.scala.sys.env("MIST_HOME"))
        val ws = new WSApi(streamer)(config.keepAliveTick)
        // order is important!
        // api router can't chain unhandled calls, because it's wrapped in cors directive
        ws.route ~ api
      }
      val http = new HttpUi(config.uiPath).route ~ DevApi.devRoutes(mainService) ~ apiv2
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


  private def runEventStreamer(config: MasterConfig)
    (implicit sys: ActorSystem, mat: ActorMaterializer): EventsStreamer = {
    val streamer = EventsStreamer(sys)

    def startStream(f: SystemEvent => Unit, name: String, close: () => Unit): Unit = {
      val doneF = streamer.eventsSource()
        .toMat(Sink.foreach(f))(Keep.right)
        .withAttributes(supervisionStrategy(resumingDecider))
        .run()
      doneF.onComplete {
        case Success(_) => logger.info(s"Event streaming for $name stopped")
        case Failure(e) => logger.error(s"Event streaming for $name was completed with error", e)
      }

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
