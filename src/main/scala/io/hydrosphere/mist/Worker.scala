package io.hydrosphere.mist

import akka.actor.{Props, ActorSystem}
import io.hydrosphere.mist.worker.ContextNode

private[mist] object Worker extends App {

  if (args.length == 0) {
    println("`name` argument is required")
    System.exit(1)
  }

  implicit val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val contextNode = system.actorOf(Props[ContextNode], name = args(0))
}
