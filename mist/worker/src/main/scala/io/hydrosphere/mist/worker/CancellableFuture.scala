package io.hydrosphere.mist.worker

import scala.concurrent.{Future, Promise}

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
          case e: InterruptedException => ps.failure(new RuntimeException("Execution was cancelled"))
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

