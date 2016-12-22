package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.jobs._
import spray.json.{JsValue, RootJsonFormat}

private[mist] trait JobConfigurationJsonSerialization extends JsonFormatSupport {

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
