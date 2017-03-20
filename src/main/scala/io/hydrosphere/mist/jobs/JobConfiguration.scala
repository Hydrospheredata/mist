package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.RouteConfig
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.JobParameters
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import spray.json.{DeserializationException, pimpString}

sealed trait JobConfiguration

object JobConfiguration {
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
}

case class FullJobConfiguration(
  path: String,
  className: String,
  namespace: String,
  parameters: JobParameters,
  externalId: Option[String],
  route: Option[String],
  action: JobConfiguration.Action = JobConfiguration.Action.Execute
) extends JobConfiguration

case class RestificatedJobConfiguration(
  route: String,
  parameters: JobParameters,
  externalId: Option[String]
) extends JobConfiguration

class FullJobConfigurationBuilder extends JobConfigurationJsonSerialization with Logger {

  private var _path: String = _
  private var _className: String = _
  private var _namespace: String = _
  private var _parameters: JobParameters = Map()
  private var _externalId: Option[String] = None
  private var _route: Option[String] = None
  private var _action: JobConfiguration.Action = JobConfiguration.Action.Execute
  
  private var _builtJob: FullJobConfiguration = _

  def setTraining(training: Boolean): FullJobConfigurationBuilder = {
    _action = if (training) JobConfiguration.Action.Train else _action
    this
  }

  def setServing(serving: Boolean): FullJobConfigurationBuilder = {
    _action = if (serving) JobConfiguration.Action.Serve else _action
    this
  }
  
  def setPath(value: String): FullJobConfigurationBuilder = {
    _path = value
    this
  }
  
  def setClassName(value: String): FullJobConfigurationBuilder = {
    _className = value
    this
  }
  
  def setNamespace(value: String): FullJobConfigurationBuilder = {
    _namespace = value
    this
  }
  
  def setParameters(value: JobParameters): FullJobConfigurationBuilder = {
    _parameters = value
    this
  }
  
  def setExternalId(value: Option[String]): FullJobConfigurationBuilder = {
    _externalId = value
    this
  }
  
  def setRoute(value: Option[String]): FullJobConfigurationBuilder = {
    _route = value
    this
  }

  def fromRouter(route: String, parameters: JobParameters, externalId: Option[String]): FullJobConfigurationBuilder = {
    _route = Some(route)
    _parameters = parameters
    _externalId = externalId
    val config = RouteConfig(route)
    _path = config.path
    _className = config.className
    _namespace = config.namespace
    this
  }

  def fromRest(path: String, body: JobParameters): FullJobConfigurationBuilder = {
    fromRouter(path, body, None)
  }

  def fromHttp(queryString: Map[String, String], body: JobParameters): FullJobConfigurationBuilder = {
    _route = None
    _parameters = body("parameters").asInstanceOf[JobParameters]
    _externalId = body.get("externalId").map(_.asInstanceOf[String])
    _path = body("path").asInstanceOf[String]
    _className = body("className").asInstanceOf[String]
    _namespace = body("namespace").asInstanceOf[String]
    this
  }
  
  def fromJson(json: String): FullJobConfigurationBuilder = {
    try {
      _builtJob =  json.parseJson.convertTo[FullJobConfiguration]
    } catch {
      case _: DeserializationException =>
        logger.debug(s"Try to parse restificated request")
        val restificatedRequest = json.parseJson.convertTo[RestificatedJobConfiguration]
        if (restificatedRequest.route.endsWith("?train")) {
          _action = JobConfiguration.Action.Train
        } else if (restificatedRequest.route.endsWith("?serve")) {
          _action = JobConfiguration.Action.Serve
        }
        _route = Some(restificatedRequest.route)
        _parameters = restificatedRequest.parameters
        _externalId = restificatedRequest.externalId

        val routeConfig = RouteConfig(restificatedRequest.route)
        _path = routeConfig.path
        _className = routeConfig.className
        _namespace = routeConfig.namespace
    }
    this
  }
  
  def init(configuration: FullJobConfiguration): FullJobConfigurationBuilder = {
    _path = configuration.path
    _className = configuration.className
    _namespace = configuration.namespace
    _parameters = configuration.parameters
    _externalId = configuration.externalId
    _route = configuration.route
    _action = configuration.action
    this
  }

  def build(): FullJobConfiguration = {
    if (_builtJob != null) {
      _builtJob
    } else {
      FullJobConfiguration(_path, _className, _namespace, _parameters, _externalId, _route, _action)
    }
  }

}

object FullJobConfigurationBuilder {

  def apply(): FullJobConfigurationBuilder = new FullJobConfigurationBuilder

}