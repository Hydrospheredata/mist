package io.hydrosphere.mist.master.logging

import akka.actor.ActorRef
import io.hydrosphere.mist.core.logging.LogEvent

trait JobLogger {
  protected val jobId: String

  def debug(message: String): Unit =
    log(LogEvent.mkDebug(jobId, message))

  def info(message: String): Unit =
    log(LogEvent.mkInfo(jobId, message))

  def warn(message: String): Unit =
    log(LogEvent.mkWarn(jobId, message))

  def error(message: String, e: Throwable): Unit =
    log(LogEvent.mkError(jobId, message, e))

  def error(message: String): Unit =
    log(LogEvent.mkError(jobId, message))

  protected def log(e: LogEvent): Unit

}

object JobLogger {

  val NOOP = new JobLogger {
    override protected def log(e: LogEvent): Unit = ()
    override protected val jobId: String = "empty"
  }

  def fromActorRef(id: String, ref: ActorRef) = new JobLogger {
    override val jobId: String = id

    def log(e: LogEvent): Unit = ref ! e
  }
}
