package io.hydrosphere.mist.utils.akka

import akka.actor.{ActorRef, ActorRefFactory, Props}

/**
  * Used to provide a factory-like function for child actors creation
  */
trait ActorF[A] {
  def createF(a: A)(fa: ActorRefFactory): ActorRef
}

/**
  * Mixin trait for actors
  * {{{
  *   class MyActor(f: ActorF[String]) extends Actor with ActorFSyntax {
  *     ...
  *     val ref = f.create("params")
  *   }
  * }}}
  */
trait ActorFSyntax {

  implicit class ActorFromFactory[A](af: ActorF[A])(implicit fa: ActorRefFactory) {
    def create(a: A): ActorRef = af.createF(a)(fa)
  }

}

object ActorF {

  def apply[A](f: (A, ActorRefFactory) => ActorRef): ActorF[A] = {
    new ActorF[A] {
      def createF(a: A)(fa: ActorRefFactory): ActorRef = f(a, fa)
    }
  }

  /**
    * For testing purposes
    */
  def static[A](ref: ActorRef): ActorF[A] = ActorF[A]((_, _) => ref)

  def props[A](f: A => Props): ActorF[A] = ActorF[A]((a, fa) => fa.actorOf(f(a)))

  def props[A, B](f: (A, B) => Props): ActorF[(A, B)] =
    ActorF.props(f.tupled)

  def props[A, B, C](f: (A, B, C) => Props): ActorF[(A, B, C)] =
    ActorF.props(f.tupled)

  def props[A, B, C, D](f: (A, B, C, D) => Props): ActorF[(A, B, C, D)] =
    ActorF.props(f.tupled)

  def props[A, B, C, D, E](f: (A, B, C, D, E) => Props): ActorF[(A, B, C, D, E)] =
    ActorF.props(f.tupled)

}
