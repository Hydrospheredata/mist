package io.hydrosphere.mist

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.actors.tools.Messages.{CreateContext, StopAllContexts}
import io.hydrosphere.mist.actors._
import io.hydrosphere.mist.jobs._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls


/** This object is entry point of Mist project */
private[mist] object Mist extends App with HTTPService {
  override implicit val system = ActorSystem("mist")
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  // TODO: Logging
  // Context creator actor
  val contextManager = system.actorOf(Props[ContextManager], name = Constants.Actors.contextManagerName)

  // Creating contexts which are specified in config as `onstart`
  for (contextName:String <- MistConfig.Contexts.precreated) {
    println("Creating contexts which are specified in config")
    contextManager ! CreateContext(contextName)
  }

  // Start HTTP server if it is on in config
  if (MistConfig.HTTP.isOn) {
    Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
  }

  // Start MQTT subscriber if it is on in config
  if (MistConfig.MQTT.isOn) {
//    system.actorOf(Props[MQTTService])
    MQTTService.subscribe(system)
  }

  // Job Recovery
  var configurationRepository: ConfigurationRepository = InMemoryJobConfigurationRepository
  lazy val jobRepository = {
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

  lazy val recoveryActor: ActorRef = system.actorOf(Props(new JobRecovery(configurationRepository, jobRepository)), name = "recoveryActor")

  if(MistConfig.MQTT.isOn && MistConfig.Recovery.recoveryOn) {
    recoveryActor ! StartRecovery
  }

  // We need to stop contexts on exit
  sys addShutdownHook {
    println("Stopping all the contexts")
    contextManager ! StopAllContexts
  }

}
