package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.utils.TypeAlias.JobParameters

sealed trait Action
object Action {

  def apply(string: String): Action = string match {
    case "execute" => Execute
    case "train" => Train
    case "serve" => Serve
  }

  case object Execute extends Action {
    override def toString: String = "execute"
  }

  case object Train extends Action {
    override def toString: String = "train"
  }

  case object Serve extends Action {
    override def toString: String = "serve"
  }
}

/**
  * Request for starting job by name
  */
case class JobExecutionRequest(
  routeId: String,
  action: Action,
  parameters: JobParameters,
  externalId: Option[String]
)

/**
  * Full configuration for job running
  */
case class JobExecutionParams(
  path: String,
  className: String,
  namespace: String,
  parameters: JobParameters,
  externalId: Option[String],
  route: Option[String],
  action: Action = Action.Execute
)

object JobExecutionParams {

  def fromDefinition(
    definition: JobDefinition,
    action: Action,
    parameters: JobParameters,
    externalId: Option[String] = None): JobExecutionParams = {

    import definition._

    JobExecutionParams(
      path = path,
      className = className,
      namespace = nameSpace,
      parameters = parameters,
      externalId = externalId,
      route = Option(name),
      action = action
    )
  }
}

