package io.hydrosphere.mist.utils

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object FutureOps {
  implicit def futureObjectSyntax(f: Future.type): FutureObjectOps = new FutureObjectOps(f)
}


final class FutureObjectOps(val f: Future.type) extends AnyVal {
  def fromTry[A](t: => Try[A]): Future[A] = t match {
    case Failure(e) => Future.failed(e)
    case Success(a) => Future.successful(a)
  }
  def fromEither[A <: Throwable, B](e: => Either[A, B]): Future[B] = e match {
    case Left(ex) => Future.failed(ex)
    case Right(b) => Future.successful(b)
  }
}
