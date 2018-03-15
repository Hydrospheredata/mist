package io.hydrosphere.mist.master.execution.workers.starter

import cats.implicits._

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.io.Source

class WrappedProcess(ps: java.lang.Process) {

  def exitValue: Option[Int] = {
    try {
      ps.exitValue().some
    } catch {
      case _: IllegalThreadStateException => None
      case e: Throwable => throw e
    }
  }

  def await(step: FiniteDuration = 1 second): Future[Unit] = {
    val promise = Promise[Unit]
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        def loop(): Unit = {
          exitValue match {
            case Some(0) => promise.success(())
            case Some(x) =>
              val out = Source.fromInputStream(ps.getInputStream).getLines().take(25).mkString("; ")
              val errOut = Source.fromInputStream(ps.getErrorStream).getLines().take(25).mkString("; ")
              promise.failure(new RuntimeException(s"Process exited with status code $x and out: $out; errOut: $errOut"))
            case None =>
          }
        }

        while(!promise.isCompleted) {
          loop()
          Thread.sleep(step.toMillis)
        }
      }
    })
    thread.setDaemon(true)
    thread.start()
    promise.future
  }
}

object WrappedProcess {

  def run(cmd: Seq[String], env: Map[String, String]): WrappedProcess = {
    import scala.collection.JavaConverters._

    val builder = new java.lang.ProcessBuilder(cmd.asJava)
    builder.environment().putAll(env.asJava)
    val ps = builder.start()
    new WrappedProcess(ps)
  }

  def run(cmd: Seq[String]): WrappedProcess = run(cmd, Map.empty)

}

