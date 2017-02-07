package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.RouteConfig
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import spray.json.{DeserializationException, pimpString}

sealed trait JobConfiguration

abstract class FullJobConfiguration extends JobConfiguration {
  def path: String
  def className: String
  def namespace: String
  def parameters: Map[String, Any] = Map()
  def externalId: Option[String] = None
  def route: Option[String] = None
}

abstract class RestificatedJobConfiguration extends JobConfiguration {
  def route: String
  def parameters: Map[String, Any] = Map()
  def externalId: Option[String] = None
}

private[mist] case class MistJobConfiguration(path: String,
                                              className: String,
                                              namespace: String,
                                              override val parameters: Map[String, Any] = Map(),
                                              override val externalId: Option[String] = None,
                                              override val route: Option[String] = None) extends FullJobConfiguration

private[mist] case class RestificatedMistJobConfiguration(route: String,
                                                          override val parameters: Map[String, Any] = Map(),
                                                          override val externalId: Option[String] = None) extends RestificatedJobConfiguration

private[mist] case class TrainingJobConfiguration(path: String,
                                                  className: String,
                                                  namespace: String,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None,
                                                  override val route: Option[String] = None) extends FullJobConfiguration

private[mist] case class ServingJobConfiguration(path: String,
                                                  className: String,
                                                  namespace: String = null,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None,
                                                  override val route: Option[String] = None) extends FullJobConfiguration


private[mist] case class RestificatedTrainingJobConfiguration(route: String,
                                                  override val parameters: Map[String, Any] = Map(),
                                                  override val externalId: Option[String] = None) extends RestificatedJobConfiguration

private[mist] case class RestificatedServingJobConfiguration(route: String,
                                                              override val parameters: Map[String, Any] = Map(),
                                                              override val externalId: Option[String] = None) extends RestificatedJobConfiguration


class FullJobConfigurationBuilder extends JobConfigurationJsonSerialization with Logger {

  private var training: Boolean = _
  private var serving: Boolean = _

  private var _path: String = _
  private var _className: String = _
  private var _namespace: String = _
  private var _parameters: Map[String, Any] = Map()
  private var _externalId: Option[String] = None
  private var _route: Option[String] = None
  
  private var _builtJob: FullJobConfiguration = _

  def setTraining(value: Boolean): FullJobConfigurationBuilder = {
    training = value
    this
  }

  def setServing(value: Boolean): FullJobConfigurationBuilder = {
    serving = value
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
  
  def setParameters(value: Map[String, Any]): FullJobConfigurationBuilder = {
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

  def fromRouter(route: String, parameters: Map[String, Any], externalId: Option[String]): FullJobConfigurationBuilder = {
    _route = Some(route)
    _parameters = parameters
    _externalId = externalId
    val config = RouteConfig(route)
    _path = config.path
    _className = config.className
    _namespace = config.namespace
    this
  }

  def fromRest(path: String, queryString: Map[String, String], body: Map[String, Any]): FullJobConfigurationBuilder = {
    fromRouter(path, body, None)
      .setServing(queryString.contains("serve"))
      .setTraining(queryString.contains("train"))
  }

  def fromHttp(queryString: Map[String, String], body: Map[String, Any]): FullJobConfigurationBuilder = {
    _route = None
    _parameters = body("parameters").asInstanceOf[Map[String, Any]]
    _externalId = body.get("externalId").map(_.asInstanceOf[String])
    _path = body("path").asInstanceOf[String]
    _className = body("className").asInstanceOf[String]
    _namespace = body("namespace").asInstanceOf[String]
    this
  }
  
  def fromJson(json: String): FullJobConfigurationBuilder = {
    try {
      _builtJob =  json.parseJson.convertTo[MistJobConfiguration]
    } catch {
      case _: DeserializationException =>
        logger.debug(s"Try to parse restificated request")
        val restificatedRequest = json.parseJson.convertTo[RestificatedMistJobConfiguration]
        if (restificatedRequest.route.endsWith("?train")) {
          training = true
        } else if (restificatedRequest.route.endsWith("?serve")) {
          serving = true
        }
        _route = Some(restificatedRequest.route)
        _parameters = restificatedRequest.parameters
        _externalId = restificatedRequest.externalId
    }
    this
  }

  def build(): FullJobConfiguration = {
    if (_builtJob != null) {
      _builtJob
    } else if (training) {
      TrainingJobConfiguration(_path, _className, _namespace, _parameters, _externalId, _route)
    } else if (serving) {
      ServingJobConfiguration(_path, _className, _namespace, _parameters, _externalId, _route)
    } else {
      MistJobConfiguration(_path, _className, _namespace, _parameters, _externalId, _route)
    }
  }

}

object FullJobConfigurationBuilder {

  def apply(): FullJobConfigurationBuilder = new FullJobConfigurationBuilder

}