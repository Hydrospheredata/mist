package io.hydrosphere.mist.master.security

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import sys.process._

object KInitLauncher {

  val NOPProcessLogger = new ProcessLogger {
    override def buffer[T](f: => T): T = f

    override def out(s: => String): Unit = ()

    override def err(s: => String): Unit = ()
  }

  case class LoopedProcess(cmd: Seq[String], interval: FiniteDuration) {

    @volatile
    private var stopped = false
    private val promise = Promise[Unit]

    def run(): Future[Unit] = {
      Try(execOnce()) match {
        case Success(0) => loop()
        case Success(x) => Future.failed(new RuntimeException(s"Kinit process failed with exit code $x"))
        case Failure(e) => Future.failed(new RuntimeException("Kinit process failed", e))
      }
    }

    private def loop():Future[Unit] = {
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          while (!stopped) {
            val code = execOnce()
            if (code != 0) {
              promise.failure(new RuntimeException(s"Kinit process failed with exit code $code"))
              stopped = true
            } else {
              Thread.sleep(interval.toMillis)
            }
          }
          promise.trySuccess(())
        }
      })
      thread.setName("kinit-loop")
      thread.start()

      promise.future
    }

    def stop(): Future[Unit] = {
      stopped = true
      promise.future
    }

    private def execOnce(): Int = Process(cmd).run(NOPProcessLogger, false).exitValue()

  }

  def create(keytab: String, principal: String, interval: FiniteDuration): LoopedProcess = {
    val cmd = Seq("kinit", "-kt", keytab, principal)
    LoopedProcess(cmd, interval)
  }
}
