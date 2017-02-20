package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.{CreateContext, StopAllContexts}
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.async.kafka.KafkaSubscriberActor
import io.hydrosphere.mist.master.async.mqtt.{MQTTServiceActor, MQTTSubscribe}
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.utils.Logger

import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
private[mist] object Master extends App with HTTPService with Logger {
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

  // Start HTTP server if it is on in config
  if (MistConfig.HTTP.isOn) {
    Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
  }

  // Start MQTT subscriber if it is on in config
  if (MistConfig.MQTT.isOn) {
    val mqttActor = system.actorOf(Props(classOf[MQTTServiceActor]))
    mqttActor ! MQTTSubscribe
  }
  
  if (MistConfig.Kafka.isOn) {
    system.actorOf(KafkaSubscriberActor.props())
  }
  
  // Start Job Queue Actor
  system.actorOf(JobQueue.props(), name = Constants.Actors.jobQueueName)

  // Job Recovery
  // TODO: start recovery
  
//  var configurationRepository: JobRepository = InMemoryJobRepository
//
//  if(MistConfig.Recovery.recoveryOn) {
//    configurationRepository = MistConfig.Recovery.recoveryTypeDb match {
//      case "MapDb" => MapDbJobRepository
//      case _ => InMemoryJobRepository
//    }
//  }
//
//  lazy val recoveryActor = system.actorOf(Props(classOf[JobRecovery], configurationRepository), name = "RecoveryActor")
//  if (MistConfig.MQTT.isOn && MistConfig.Recovery.recoveryOn) {
//    recoveryActor ! StartRecovery
//  }

  // We need to stop contexts on exit
  sys addShutdownHook {
    logger.info("Stopping all the contexts")
    workerManager ! StopAllContexts()
  }
}
