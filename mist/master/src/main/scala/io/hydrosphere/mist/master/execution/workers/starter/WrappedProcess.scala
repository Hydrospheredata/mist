package io.hydrosphere.mist.master.execution.workers.starter

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.{Files, Path, StandardOpenOption}

import cats.implicits._
import io.hydrosphere.mist.utils.Logger
import org.apache.commons.io.IOUtils

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.io.Source
import scala.util.Try

case class WrappedProcess(
  ps: java.lang.Process,
  buildErrorMsg: Int => String
) extends Logger {

  def exitValue: Option[Int] = {
    try {
      ps.exitValue().some
    } catch {
      case _: IllegalThreadStateException => None
      case e: Throwable => throw e
    }
  }

  private def mkThread(f: => Unit): Thread = {
    val th = new Thread(new Runnable {
      override def run(): Unit = f
    })
    th.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        logger.error(s"Handled error from process wrapper", e)
      }
    })
    th.setDaemon(true)
    th
  }

  def saveOut(out: Path): WrappedProcess = {
    val thread = mkThread {
      val outStream = ps.getInputStream
      val buffer = new Array[Byte](8096)

      if (!Files.exists(out)) Files.createFile(out)
      val bos = new BufferedOutputStream(new FileOutputStream(out.toFile))

      var read = 0
      while(read >= 0) {
        read = outStream.read(buffer)
        if (read > 0) {
          bos.write(buffer, 0, read)
          bos.flush()
        } else {
          Thread.sleep(100)
        }
      }

      bos.close()
      outStream.close()
    }
    thread.start()

    new WrappedProcess(ps, exitCode => {
      val stdOut = Source.fromFile(out.toFile).getLines().take(25).mkString(";")
      s"Process exited with status code $exitCode and out: $stdOut"
    })
  }

  def await(step: FiniteDuration = 1 second): Future[Unit] = {
    val promise = Promise[Unit]
    val thread = mkThread {
      def loop(): Unit = {
        exitValue match {
          case Some(0) => promise.success(())
          case Some(x) => promise.failure(new RuntimeException(buildErrorMsg(x)))
          case None =>
        }
      }

      while(!promise.isCompleted) {
        loop()
        Thread.sleep(step.toMillis)
      }
    }
    thread.start()
    promise.future
  }
}

object WrappedProcess {

  def started(ps: java.lang.Process): WrappedProcess = {
    WrappedProcess(ps, exitValue => {
      val out = Source.fromInputStream(ps.getInputStream).getLines().take(25).mkString("; ")
      s"Process exited with status code $exitValue and out: $out"
    })
  }

  def run(cmd: Seq[String], env: Map[String, String], out: Option[Path]): Try[WrappedProcess] = {
    import scala.collection.JavaConverters._

    Try {
      val builder = new java.lang.ProcessBuilder(cmd.asJava)
      builder.environment().putAll(env.asJava)
      builder.redirectErrorStream(true)
      val ps = builder.start()

      val wp = started(ps)
      out match {
        case Some(p) => wp.saveOut(p)
        case None => wp
      }
    }
  }

  def run(cmd: Seq[String], env: Map[String, String], out: Path): Try[WrappedProcess] = run(cmd, env, Some(out))
  def run(cmd: Seq[String], out: Path): Try[WrappedProcess] = run(cmd, Map.empty[String, String], Some(out))
  def run(cmd: Seq[String]): Try[WrappedProcess] = run(cmd, Map.empty[String, String], None)

}

