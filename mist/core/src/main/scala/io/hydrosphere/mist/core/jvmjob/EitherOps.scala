package io.hydrosphere.mist.core.jvmjob

/**
  * We don't need full cats library for worker - keep worker artifact with minimal dependency
  * Taken from cats.syntax.EitherOps
  */
object EitherOps {

  implicit def eitherOpsSyntax[A, B](e: Either[A, B]): EitherOps[A, B] = new EitherOps(e)

  implicit def eitherObjectSyntax(either: Either.type): EitherObjectOps = new EitherObjectOps(either)
}

final class EitherOps[A, B](val e: Either[A, B]) extends AnyVal {

  def map[C](f: B => C): Either[A, C] = e match {
    case l @ Left(_) => l.asInstanceOf[Left[A, C]]
    case Right(b) => Right(f(b))
  }

  def flatMap[AA >: A, C](f: B => Either[AA, C]): Either[AA, C] = e match {
    case l @ Left(_) => l.asInstanceOf[Left[AA, C]]
    case Right(b)    => f(b)
  }

}

final class EitherObjectOps(val either: Either.type) extends AnyVal {

  def catchAll[A](f: => A): Either[Throwable, A] = {
    try {
      Right(f)
    } catch {
      case e: Throwable => Left(e)
    }
  }

  def catchNonFatal[A](f: => A): Either[Throwable, A] =
    try {
      Right(f)
    } catch {
      case scala.util.control.NonFatal(t) => Left(t)
    }
}

