package io.hydrosphere.mist.master.models

import io.hydrosphere.mist.jobs.Action

/** Specify how use context/workers */
sealed trait RunMode

object RunMode {

  /** Job will share one worker with jobs that are running on the same namespace */
  case object Default extends RunMode
  /** There will be created unique worker for job execution */
  case class UniqueContext(id: Option[String]) extends RunMode

  def fromString(s: String): Option[RunMode] = s match {
    case "default" => Some(Default)
    case "uniqueContext" => Some(UniqueContext(None))
  }

}

case class RunSettings(
  contextId: Option[String],
  mode: RunMode
)

object RunSettings {

  val Default = RunSettings(None, RunMode.Default)

}

/**
  * Request for starting job by name
  * New version api
  */
case class JobStartRequest(
  routeId: String,
  parameters: Map[String, Any],
  externalId: Option[String] = None,
  runSettings: RunSettings = RunSettings.Default
)

case class JobStartResponse(id: String)

/**
  * Like JobStartRequest, but for async interfaces
  * (spray json not support default values)
  */
case class AsyncJobStartRequest(
  routeId: String,
  parameters: Option[Map[String, Any]],
  externalId: Option[String],
  runSettings: Option[RunSettings]
) {

  def toCommon: JobStartRequest =
    JobStartRequest(
      routeId,
      parameters.getOrElse(Map.empty),
      externalId,
      runSettings.getOrElse(RunSettings.Default))
}
