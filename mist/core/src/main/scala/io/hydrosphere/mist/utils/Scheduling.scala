package io.hydrosphere.mist.utils

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

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

    override def tick[A](d: FiniteDuration)(value: => A): Future[A] = {
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

