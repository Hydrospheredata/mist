package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.{CreateContext, StopAllContexts}
import io.hydrosphere.mist.jobs.{ConfigurationRepository, InMapDbJobConfigurationRepository, InMemoryJobConfigurationRepository}
import io.hydrosphere.mist.master.mqtt.{MQTTServiceActor, MQTTSubscribe}
import io.hydrosphere.mist.master.{ClusterManager, HTTPService, JobRecovery, StartRecovery}
import io.hydrosphere.mist.utils.Logger

import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
private[mist] object Master extends App with HTTPService with Logger {
  override implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  logger.info(MistConfig.Akka.Worker.port.toString)
  logger.info(MistConfig.Akka.Main.port.toString)

  // Context creator actor
  val workerManager = system.actorOf(Props[ClusterManager], name = Constants.Actors.workerManagerName)

    // Creating contexts which are specified in config as `onstart`
  MistConfig.Contexts.precreated foreach { contextName =>
    logger.info("Creating contexts which are specified in config")
    workerManager ! CreateContext(contextName)
  }

  // Start HTTP server if it is on in config
  if (MistConfig.HTTP.isOn) {
    Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
  }

  // Start MQTT subscriber if it is on in config
  if (MistConfig.MQTT.isOn) {
    val mqttActor = system.actorOf(Props(classOf[MQTTServiceActor]))
    mqttActor ! MQTTSubscribe
  }

  // Job Recovery
  var configurationRepository: ConfigurationRepository = InMemoryJobConfigurationRepository

  if(MistConfig.Recovery.recoveryOn) {
    configurationRepository = MistConfig.Recovery.recoveryTypeDb match {
      case "MapDb" => InMapDbJobConfigurationRepository
      case _ => InMemoryJobConfigurationRepository
    }
  }

  lazy val recoveryActor = system.actorOf(Props(classOf[JobRecovery], configurationRepository), name = "RecoveryActor")
  if (MistConfig.MQTT.isOn && MistConfig.Recovery.recoveryOn) {
    recoveryActor ! StartRecovery
  }

  // We need to stop contexts on exit
  sys addShutdownHook {
    logger.info("Stopping all the contexts")
    workerManager ! StopAllContexts
  }
}
