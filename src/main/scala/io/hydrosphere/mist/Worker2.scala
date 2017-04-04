package io.hydrosphere.mist

import io.hydrosphere.mist.master.namespace.RemoteWorker
import akka.actor.ActorSystem

object Worker2 extends App {

  val name = args(0)

  val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)
  val props = RemoteWorker.props(name)
  system.actorOf(props, s"worker-$name")

}
