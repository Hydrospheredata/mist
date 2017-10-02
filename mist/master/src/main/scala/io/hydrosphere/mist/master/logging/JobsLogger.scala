package io.hydrosphere.mist.master.logging

import akka.actor.ActorRef
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent

trait JobsLogger {

  def debug(jobId: String, message: String): Unit =
    log(LogEvent.mkDebug(jobId, message))

  def info(jobId: String, message: String): Unit =
    log(LogEvent.mkInfo(jobId, message))

  def warn(jobId: String, message: String): Unit =
    log(LogEvent.mkWarn(jobId, message))

  def error(jobId: String, message: String, e: Throwable): Unit =
    log(LogEvent.mkError(jobId, message, e))

  protected def log(e: LogEvent): Unit

}

object JobsLogger {

  val NOOPLogger = new JobsLogger {
    def log(e: LogEvent): Unit = {}
  }

  def fromActorRef(ref: ActorRef) = new JobsLogger {
    def log(e: LogEvent): Unit = ref ! e
  }
}
