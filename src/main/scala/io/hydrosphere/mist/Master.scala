package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.StopAllContexts
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.async.AsyncInterface
import io.hydrosphere.mist.master.cluster.{CliResponder, ClusterManager}
import io.hydrosphere.mist.utils.Logger

import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
private[mist] object Master extends App with HttpService with Logger {
  override implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  logger.info(MistConfig.Akka.Worker.port.toString)
  logger.info(MistConfig.Akka.Main.port.toString)

  // Context creator actor
  val workerManager = system.actorOf(Props[ClusterManager], name = Constants.Actors.clusterManagerName)

  logger.debug(system.toString)

  // Creating contexts which are specified in config as `onstart`
  MistConfig.Contexts.precreated foreach { contextName =>
    logger.info("Creating contexts which are specified in config")
    workerManager ! ClusterManager.CreateContext(contextName)
  }

  // Start HTTP server if it is on in config
  val clientRouteLogged = DebuggingDirectives.logRequestResult("Client ReST", Logging.InfoLevel)(route)
  if (MistConfig.Http.isOn) {
    Http().bindAndHandle(clientRouteLogged, MistConfig.Http.host, MistConfig.Http.port)
  }
  
  // Start CLI
  system.actorOf(CliResponder.props(), name = Constants.Actors.cliResponderName)

  AsyncInterface.init(system)

  // Start MQTT subscriber
  if (MistConfig.Mqtt.isOn) {
    AsyncInterface.subscriber(AsyncInterface.Provider.Mqtt)
  }
  
  // Start Kafka subscriber
  logger.debug(MistConfig.Kafka.isOn.toString)
  if (MistConfig.Kafka.isOn) {
    AsyncInterface.subscriber(AsyncInterface.Provider.Kafka)
  }
  
  // Start Job Queue Actor
  system.actorOf(JobQueue.props())

  // Start job recovery
  system.actorOf(JobRecovery.props()) ! JobRecovery.StartRecovery
  
  // We need to stop contexts on exit
  sys addShutdownHook {
    logger.info("Stopping all the contexts")
    workerManager ! StopAllContexts()
  }
}
