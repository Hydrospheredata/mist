package com.provectus.lymph

import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.provectus.lymph.actors.tools.Messages.{CreateContext, RemoveAllContexts}
import com.provectus.lymph.actors.{ContextManager, MQTTService, HTTPService}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls


/** This object is entry point of Lymph project
  */
private[lymph] object Lymph extends App with HTTPService {
  override implicit val system = ActorSystem("lymph")
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  // Context creator actor
  val contextManager = system.actorOf(Props[ContextManager], name = "ContextManager")

  // Creating contexts which are specified in config as `onstart`
  for (contextName:String <- LymphConfig.Contexts.precreated) {
    println("Creating contexts which are specified in config")
    contextManager ! CreateContext(contextName)
  }

  // Start HTTP server if it is on in config
  if (LymphConfig.HTTP.isOn) {
    Http().bindAndHandle(route, LymphConfig.HTTP.host, LymphConfig.HTTP.port)
  }

  // Start MQTT subscriber if it is on in config
  if (LymphConfig.MQTT.isOn) {
    system.actorOf(Props[MQTTService])
  }

  // We need to stop contexts on exit
  sys addShutdownHook {
    println("Stopping all the contexts")
    contextManager ! RemoveAllContexts
  }

}
