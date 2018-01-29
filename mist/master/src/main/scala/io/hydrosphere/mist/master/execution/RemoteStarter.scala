package io.hydrosphere.mist.master.execution

import akka.actor.ActorRef
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.Future

trait RemoteStarter {

  def start(context: ContextConfig): Future[ActorRef]

}

object RemoteStarter {

}