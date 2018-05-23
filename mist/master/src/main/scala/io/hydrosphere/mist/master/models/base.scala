package io.hydrosphere.mist.master.models

import scala.concurrent.duration.Duration

/** Specify how use context/workers */
sealed trait RunMode {

  def name: String = this match {
    case RunMode.Shared => "shared"
    case RunMode.ExclusiveContext => "exclusive"
  }

}

object RunMode {

  def fromName(n: String): RunMode = n match {
    case "shared" => RunMode.Shared
    case "exclusive" => RunMode.ExclusiveContext
    case x => throw new IllegalArgumentException(s"Unknown mode $x")
  }
  /** Job will share one worker with jobs that are running on the same namespace */
  case object Shared extends RunMode
  /** There will be created unique worker for job execution */
  case object ExclusiveContext extends RunMode

}

trait NamedConfig {
  val name: String
}

case class ContextConfig(
  name: String,
  sparkConf: Map[String, String],
  downtime: Duration,
  maxJobs: Int,
  precreated: Boolean,
  runOptions: String,
  workerMode: RunMode,
  streamingDuration: Duration,
  maxConnFailures: Int
) extends NamedConfig {

  def maxJobsOnNode: Int = workerMode match {
    case RunMode.Shared => maxJobs
    case RunMode.ExclusiveContext => 1
  }

}

case class FunctionConfig(
  name: String,
  path: String,
  className: String,
  defaultContext: String
) extends NamedConfig
