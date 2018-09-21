package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorRef}

import scala.concurrent.Future

trait FutureSubscribe extends Actor {

  private var realRef: Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()
    realRef = Some(self)
  }

  override def postStop(): Unit = {
    super.postStop()
    realRef = None
  }

  def subscribe0[A, B, C](future: Future[A])(f: A => B, errHandle: Throwable => C): Unit = {
    future.onComplete(res => {
      realRef match {
        case None =>
        case Some(ref) =>
          val msg = res match {
            case scala.util.Success(v) => f(v)
            case scala.util.Failure(e) => errHandle(e)
          }
          ref ! msg
      }
    })(context.dispatcher)
  }

  def subscribe[A, B](future: Future[A])(f: A => B): Unit = subscribe0(future)(f, e => akka.actor.Status.Failure(e))

  def subscribeId[A](future: Future[A]): Unit = subscribe(future)(identity[A])

}

