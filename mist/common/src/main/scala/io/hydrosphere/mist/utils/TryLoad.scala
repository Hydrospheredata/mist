package io.hydrosphere.mist.utils

import scala.util.control.ControlThrowable

object NonFatal {
  def apply(t: Throwable): Boolean = t match {
    case _: LinkageError => true
    case _: VirtualMachineError | _: ThreadDeath | _: InterruptedException | _: ControlThrowable => false
    case _ => true
  }

  def unapply(t: Throwable): Option[Throwable] = if (apply(t)) Some(t) else None
}

sealed trait TryLoad[+A] { self =>

  def map[B](f: A => B): TryLoad[B] =
    self match {
      case Succ(a) => TryLoad(f(a))
      case err: Err[_] => err.asInstanceOf[Err[B]]
    }

  def flatMap[B](f: A => TryLoad[B]): TryLoad[B] =
    self match {
      case Succ(a) => TryLoad(f(a)).flatten
      case err: Err[_] => err.asInstanceOf[Err[B]]
    }

  def flatten[B](implicit ev: A <:< TryLoad[B]): TryLoad[B]

  def orElse[B >: A](f: => TryLoad[B]): TryLoad[B] =
    self match {
      case succ: Succ[_] => succ.asInstanceOf[Succ[B]]
      case err: Err[_] => TryLoad(f).flatten
    }

  def isSuccess: Boolean
  def isFailure: Boolean = !isSuccess

  def get: A
}

object TryLoad {

  def apply[A](f: => A): TryLoad[A] =
    try { Succ(f) } catch {
      case NonFatal(e) => Err(e)
    }

  def fromEither[A](ei: Either[Throwable, A]): TryLoad[A] = ei match {
    case Right(v) => Succ(v)
    case Left(err) => Err(err)
  }

}

final case class Succ[+A](value: A) extends TryLoad[A] {
  def flatten[B](implicit ev: <:<[A, TryLoad[B]]): TryLoad[B] = value
  def isSuccess: Boolean = true
  def get: A = value
}
final case class Err[+A](err: Throwable) extends TryLoad[A] {
  def flatten[B](implicit ev: <:<[A, TryLoad[B]]): TryLoad[B] = this.asInstanceOf[TryLoad[B]]
  def isSuccess: Boolean = false
  def get: A = throw err
}
