package io.hydrosphere.mist.utils

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import java.util.{Timer, TimerTask}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
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

trait Scheduling {
  def tick[A](d: FiniteDuration)(value: => A): Future[A]
  def delay(d: FiniteDuration): Future[Unit] = tick(d)(())
  def close(): Unit
}

object Scheduling {

  /**
    * Uses java.util.concurrent.ScheduledThreadPoolExecutor
    */
  def stpBased(pollSize: Int): Scheduling = new Scheduling {

    private val stp = new ScheduledThreadPoolExecutor(pollSize)

    override def futureTick[A](d: FiniteDuration)(value: => A): Future[A] = {
      val p = Promise[A]
      val r = new Runnable {
        override def run(): Unit = p.success(value)
      }
      stp.schedule(r, d.toMillis, TimeUnit.MILLISECONDS)
      p.future
    }

    override def close(): Unit = stp.shutdown()
  }
}
