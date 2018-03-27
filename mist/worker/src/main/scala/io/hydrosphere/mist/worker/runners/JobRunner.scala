package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.NamedContext
import mist.api.data.JsLikeData

import scala.concurrent.{Future, Promise}
import scala.util._

trait CancellableFuture[A] {
  def future: Future[A]
  def cancel(): Unit
}

object CancellableFuture {

  def onDetachedThread[A](f: => A): CancellableFuture[A] = {
    val ps = Promise[A]
    val runnable = new Runnable {
      override def run(): Unit = {
        try {
          ps.success(f)
        } catch {
          case e: Throwable => ps.failure(e)
        }
      }
    }
    val thread = new Thread(runnable)
    thread.setDaemon(true)
    thread.start()

    new CancellableFuture[A] {
      override def cancel(): Unit = thread.interrupt()
      override def future: Future[A] = ps.future
    }
  }
}

trait JobRunner {

  def runSync(req: RunJobRequest, context: NamedContext): Either[Throwable, JsLikeData]

  def runDetached(req: RunJobRequest, context: NamedContext): CancellableFuture[JsLikeData] = {
    def handleInterrupted(e: Throwable): Throwable = e match {
      case _: InterruptedException => new RuntimeException("Job was cancelled")
      case e => e
    }

    CancellableFuture.onDetachedThread {
      runSync(req, context) match {
        case Left(e) => throw handleInterrupted(e)
        case Right(data) => data
      }
    }
  }

}


