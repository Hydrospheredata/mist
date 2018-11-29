package io.hydrosphere.mist.utils.akka

import akka.actor._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

class WhenTerminated(
  ref: ActorRef,
  promise: Promise[Unit]
) extends Actor {

  override def preStart: Unit = {
    context.watch(ref)
  }

  def receive: Receive = {
    case Terminated(_) =>
      promise.success(())
      context.stop(self)
  }
}

object WhenTerminated {

  def apply(ref: ActorRef)(implicit fa: ActorRefFactory): Future[Unit] = {
    val promise = Promise[Unit]
    val props = Props(classOf[WhenTerminated], ref, promise)
    fa.actorOf(props)
    promise.future
  }

  def apply(ref: ActorRef, action: => Unit)(implicit fa: ActorRefFactory): Unit =
    apply(ref).onComplete(_ => action)

}
