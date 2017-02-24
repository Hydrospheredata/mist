package io.hydrosphere.mist.utils

import akka.actor.Actor
import akka.actor.Actor.Receive

private[mist] trait MultiReceiveActor {
  var receivers: Receive = Actor.emptyBehavior

  def receiver(next: Actor.Receive) {
    receivers = receivers orElse next
  }

  def receive: Receive = receivers
}
