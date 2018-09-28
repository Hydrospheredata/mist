package io.hydrosphere.mist.master.execution.aws

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import cats.effect.{Clock, IO, Timer}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class IOTimer(
  ec: ExecutionContext,
  sc: ScheduledExecutorService
) extends Timer[IO] {

  override val clock: Clock[IO] = new Clock[IO] {
    override def realTime(unit: TimeUnit): IO[Long] =
      IO(unit.convert(System.currentTimeMillis(), MILLISECONDS))

    override def monotonic(unit: TimeUnit): IO[Long] =
      IO(unit.convert(System.nanoTime(), NANOSECONDS))
  }

  override def sleep(timespan: FiniteDuration): IO[Unit] =
    IO.cancelable { cb =>
      val tick = new Runnable {
        def run() = ec.execute(new Runnable {
          def run() = cb(Right(()))
        })
      }
      val f = sc.schedule(tick, timespan.length, timespan.unit)
      IO(f.cancel(false))
    }
}

object IOTimer {

  implicit val Default = new IOTimer(ExecutionContext.global, Executors.newScheduledThreadPool(2))

}
