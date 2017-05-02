package io.hydrosphere.mist.master.models

/** Specify how use context/workers */
sealed trait RunMode

object RunMode {

  /** Job will share one worker with jobs that are running on the same namespace */
  case object Default extends RunMode
  /** There will be created unique worker for job exection */
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

/**
  * Request for starting job by name
  * New version api
  */
case class JobStartRequest(
  routeId: String,
  parameters: Map[String, Any],
  externalId: Option[String],
  runSettings: RunSettings
)

case class JobStartResponse(id: String)

