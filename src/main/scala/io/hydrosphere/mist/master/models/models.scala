package io.hydrosphere.mist.master.models

/** Specify how use context/workers */
sealed trait RunMode {

  def name: String = this match {
    case RunMode.Default => "shared"
    case e:RunMode.ExclusiveContext => "exclusive"
  }
}

object RunMode {

  /** Job will share one worker with jobs that are running on the same namespace */
  case object Default extends RunMode
  /** There will be created unique worker for job execution */
  case class ExclusiveContext(id: Option[String]) extends RunMode

  def fromString(s: String): Option[RunMode] = s match {
    case "shared" => Some(Default)
    case "exclusive" => Some(ExclusiveContext(None))
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
  endpointId: String,
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
  endpointId: String,
  parameters: Option[Map[String, Any]],
  externalId: Option[String],
  runSettings: Option[RunSettings]
) {

  def toCommon: JobStartRequest =
    JobStartRequest(
      endpointId,
      parameters.getOrElse(Map.empty),
      externalId,
      runSettings.getOrElse(RunSettings.Default))
}
