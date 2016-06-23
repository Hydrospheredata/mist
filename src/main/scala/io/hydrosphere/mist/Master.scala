package io.hydrosphere.mist

import java.net.URLEncoder

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.actors.tools.Messages.{CreateContext, StopAllContexts}
import io.hydrosphere.mist.actors._
import io.hydrosphere.mist.jobs._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
private[mist] object Master extends App with HTTPService {
  override implicit val system = ActorSystem("mist", MistConfig.Akka.Main.settings)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  println(MistConfig.Akka.Worker.port)
  println(MistConfig.Akka.Main.port)

  // TODO: Logging
  // Context creator actor
  val workerManager = system.actorOf(Props[WorkerManager], name = Constants.Actors.workerManagerName)

  // Creating contexts which are specified in config as `onstart`
  for (contextName:String <- MistConfig.Contexts.precreated) {
    println("Creating contexts which are specified in config")
    workerManager ! CreateContext(contextName)
  }

  // Start HTTP server if it is on in config
  if (MistConfig.HTTP.isOn) {
    Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
  }

  // Start MQTT subscriber if it is on in config
  if (MistConfig.MQTT.isOn) {
    val mqttActor = system.actorOf(Props(classOf[MQTTServiceActor]))
    mqttActor ! MqttSubscribe
  }

  // Job Recovery
  var configurationRepository: ConfigurationRepository = InMemoryJobConfigurationRepository
  val jobRepository = {
    MistConfig.Recovery.recoveryOn match {
      case true => {
        configurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
        }
        RecoveryJobRepository
      }
      case _ => InMemoryJobRepository
    }
  }

  lazy val recoveryActor = system.actorOf(Props(classOf[JobRecovery], configurationRepository, jobRepository))
  if (MistConfig.MQTT.isOn && MistConfig.Recovery.recoveryOn) {
    recoveryActor ! StartRecovery
  }

  // We need to stop contexts on exit
  sys addShutdownHook {
    println("Stopping all the contexts")
    workerManager ! StopAllContexts
  }
}
