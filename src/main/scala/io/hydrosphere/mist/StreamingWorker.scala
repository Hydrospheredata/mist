package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import io.hydrosphere.mist.worker.StreamingNode

private[mist] object StreamingWorker extends App with Logger{

  if (args.length < 4) {
    logger.error("`path` `className` `name` `externalId` arguments is required")
    System.exit(1)
  }

  implicit val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val contextNode = system.actorOf(Props(new StreamingNode(args(0), args(1), args(2), args(3))), name = "StreamingJobStarter")
}
