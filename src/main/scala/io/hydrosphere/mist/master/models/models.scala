package io.hydrosphere.mist.master.models

import java.util.UUID

import io.hydrosphere.mist.jobs.{Action, JobDetails}

/** Specify how use context/workers */
sealed trait RunMode {

  def name: String = this match {
    case RunMode.Shared => "shared"
    case e:RunMode.ExclusiveContext => "exclusive"
  }

}

object RunMode {

  /** Job will share one worker with jobs that are running on the same namespace */
  case object Shared extends RunMode
  /** There will be created unique worker for job execution */
  case class ExclusiveContext(id: Option[String]) extends RunMode

}

case class RunSettings(
  /** Context name that overrides endpoint context */
  contextId: Option[String],
  /** Worker name postfix */
  workerId: Option[String]
)

object RunSettings {

  val Default = RunSettings(None, None)

}

/**
  * Request for starting job by name
  * New version api
  */
case class EndpointStartRequest(
  endpointId: String,
  parameters: Map[String, Any],
  externalId: Option[String] = None,
  runSettings: RunSettings = RunSettings.Default,
  id: String = UUID.randomUUID().toString
)

case class JobStartRequest(
  id: String,
  endpoint: EndpointConfig,
  context: ContextConfig,
  parameters: Map[String, Any],
  runMode: RunMode,
  source: JobDetails.Source,
  externalId: Option[String],
  action: Action = Action.Execute
)

case class JobStartResponse(id: String)

/**
  * Like JobStartRequest, but for async interfaces
  * (spray json not support default values)
  */
case class AsyncEndpointStartRequest(
  endpointId: String,
  parameters: Option[Map[String, Any]],
  externalId: Option[String],
  runSettings: Option[RunSettings]
) {

  def toCommon: EndpointStartRequest =
    EndpointStartRequest(
      endpointId,
      parameters.getOrElse(Map.empty),
      externalId,
      runSettings.getOrElse(RunSettings.Default))
}


case class DevJobStartRequest(
  fakeName: String,
  path: String,
  className: String,
  parameters: Map[String, Any],
  externalId: Option[String],
  workerId: Option[String],
  context: String
)

case class DevJobStartRequestModel(
  fakeName: String,
  path: String,
  className: String,
  parameters: Option[Map[String, Any]],
  externalId: Option[String],
  workerId: Option[String],
  context: String
) {

  def toCommon: DevJobStartRequest = {
    DevJobStartRequest(
      fakeName = fakeName,
      path = path,
      className = className,
      parameters = parameters.getOrElse(Map.empty),
      externalId = externalId,
      workerId = workerId,
      context = context
    )
  }
}
