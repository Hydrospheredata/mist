package io.hydrosphere.mist.master.models

import java.util.UUID

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.FunctionInfoData
import io.hydrosphere.mist.master.JobDetails
import mist.api.data.{JsData, JsMap}

import scala.concurrent.duration.Duration


case class RunSettings(
  /** Context name that overrides endpoint context */
  contextId: Option[String]
)

object RunSettings {

  val Default = RunSettings(None)

}

case class Timeouts(start: Duration, perform: Duration)
object Timeouts {
  val Infinity = Timeouts(Duration.Inf, Duration.Inf)
}

/**
  * Request for starting job by name
  * New version api
  */
case class FunctionStartRequest(
  functionId: String,
  parameters: JsMap,
  externalId: Option[String] = None,
  runSettings: RunSettings = RunSettings.Default,
  id: String = UUID.randomUUID().toString,
  timeouts: Timeouts = Timeouts.Infinity
)

case class JobStartRequest(
  id: String,
  function: FunctionInfoData,
  context: ContextConfig,
  parameters: JsMap,
  source: JobDetails.Source,
  externalId: Option[String],
  action: Action = Action.Execute,
  timeouts: Timeouts = Timeouts.Infinity
)

case class JobStartResponse(id: String)

/**
  * Like JobStartRequest, but for async interfaces
  * (spray json not support default values)
  */
case class AsyncFunctionStartRequest(
  functionId: String,
  parameters: Option[JsMap],
  externalId: Option[String],
  runSettings: Option[RunSettings]
) {

  def toCommon: FunctionStartRequest =
    FunctionStartRequest(
      functionId,
      parameters.getOrElse(JsMap.empty),
      externalId,
      runSettings.getOrElse(RunSettings.Default))
}


case class DevJobStartRequest(
  fakeName: String,
  path: String,
  className: String,
  parameters: JsMap,
  externalId: Option[String],
  workerId: Option[String],
  context: String
)

case class DevJobStartRequestModel(
  fakeName: String,
  path: String,
  className: String,
  parameters: Option[JsMap],
  externalId: Option[String],
  workerId: Option[String],
  context: String
) {

  def toCommon: DevJobStartRequest = {
    DevJobStartRequest(
      fakeName = fakeName,
      path = path,
      className = className,
      parameters = parameters.getOrElse(JsMap.empty),
      externalId = externalId,
      workerId = workerId,
      context = context
    )
  }
}
