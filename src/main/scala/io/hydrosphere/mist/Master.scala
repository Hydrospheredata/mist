package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.{CreateContext, StopAllContexts}
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.async.AsyncInterface
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

  // Creating contexts which are specified in config as `onstart`
  MistConfig.Contexts.precreated foreach { contextName =>
    logger.info("Creating contexts which are specified in config")
    workerManager ! CreateContext(contextName)
  }

  // Start HTTP server
  if (MistConfig.Http.isOn) {
    Http().bindAndHandle(route, MistConfig.Http.host, MistConfig.Http.port)
  }
  
  AsyncInterface.init(system)

  // Start MQTT subscriber
  if (MistConfig.Mqtt.isOn) {
    AsyncInterface.subscriber(AsyncInterface.Provider.Mqtt)
  }
  
  // Start Kafka subscriber
  if (MistConfig.Kafka.isOn) {
    AsyncInterface.subscriber(AsyncInterface.Provider.Kafka)
  }
  
  // Start Job Queue Actor
  system.actorOf(JobQueue.props(), name = Constants.Actors.jobQueueName)

  // Start job recovery
  system.actorOf(JobRecovery.props()) ! JobRecovery.StartRecovery
  
  // We need to stop contexts on exit
  sys addShutdownHook {
    logger.info("Stopping all the contexts")
    workerManager ! StopAllContexts()
  }
}
