package io.hydrosphere.mist.utils

import io.hydrosphere.mist.jobs._
import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat, deserializationError, serializationError}

private[mist] trait JsonFormatSupport extends DefaultJsonProtocol {

  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown: Any => serializationError("Do not understand object of type " + unknown.getClass.getName)
    }

    def read(value: JsValue): Any = value match {
      case JsNumber(number) => number.toBigInt()
      case JsString(string) => string
      case _: JsArray => listFormat[Any].read(value)
      case _: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown: Any => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }

  implicit object FullJobConfigurationFormat extends RootJsonFormat[FullJobConfiguration] {
    def write(x: FullJobConfiguration): JsValue = x match {
      case mistJobConfiguration: MistJobConfiguration => mistJobConfigurationFormat.write(mistJobConfiguration)
      case trainingJobConfiguration: TrainingJobConfiguration => trainingJobConfigurationFormat.write(trainingJobConfiguration)
    }

    def read(v: JsValue): FullJobConfiguration = v.convertTo[MistJobConfiguration]
  }

  implicit val mistJobConfigurationFormat: RootJsonFormat[MistJobConfiguration] = jsonFormat5(MistJobConfiguration)
  implicit val mistJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedMistJobConfiguration] = jsonFormat3(RestificatedMistJobConfiguration)
  implicit val trainingJobConfigurationFormat: RootJsonFormat[TrainingJobConfiguration] = jsonFormat5(TrainingJobConfiguration)
  implicit val trainingJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedTrainingJobConfiguration] = jsonFormat3(RestificatedTrainingJobConfiguration)
  implicit val servingJobConfigurationFormat: RootJsonFormat[ServingJobConfiguration] = jsonFormat5(ServingJobConfiguration)
  implicit val servingJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedServingJobConfiguration] = jsonFormat3(RestificatedServingJobConfiguration)
  
  implicit val jobResultFormat: RootJsonFormat[JobResult] = jsonFormat4(JobResult)

  sealed trait JobConfigError

  case class NoRouteError(reason: String) extends JobConfigError

  case class ConfigError(reason: String) extends JobConfigError

}