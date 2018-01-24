package io.hydrosphere.mist.utils.akka

import akka.actor._

class WhenTerminated(
  ref: ActorRef,
  f: => Unit
) extends Actor {

  override def preStart: Unit = {
    context.watch(ref)
  }

  def receive: Receive = {
    case Terminated(_) =>
      f
      context.stop(self)
  }
}

object WhenTerminated {

  def apply(ref: ActorRef, action: => Unit)(implicit f: ActorRefFactory): ActorRef = {
    val props = Props(classOf[WhenTerminated], ref, () => action)
    f.actorOf(props)
  }

}
