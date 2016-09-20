package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import io.hydrosphere.mist.worker.ContextNode

private[mist] object Worker extends App with Logger{

  if (args.length == 0) {
    logger.error("`name` argument is required")
    System.exit(1)
  }

  implicit val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val contextNode = system.actorOf(ContextNode.props(args(0)), name = args(0))
}
