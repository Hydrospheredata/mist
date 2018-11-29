package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorRef}

import scala.concurrent.Future

trait ActorFutureHandler { that: Actor =>

  private var realRef: Option[ActorRef] = None

  override def preStart(): Unit = {
    that.preStart()
    realRef = Some(self)
  }

  override def postStop(): Unit = {
    that.postStop()
    realRef = None
  }

  def subscribe[A, B, C](future: Future[A])(f: A => B, g: Throwable => C): Unit = {
    future.onComplete(res => {
      realRef match {
        case None =>
        case Some(ref) =>
          val msg = res match {
            case scala.util.Success(v) => f(v)
            case scala.util.Failure(e) => g(e)
          }
          ref ! msg
      }
    })(context.dispatcher)
  }
}
