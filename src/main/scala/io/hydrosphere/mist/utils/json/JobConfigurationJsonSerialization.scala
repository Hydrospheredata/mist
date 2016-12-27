package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.jobs._
import spray.json.{JsValue, RootJsonFormat}

private[mist] trait JobConfigurationJsonSerialization extends JsonFormatSupport {

  implicit object FullJobConfigurationFormat extends RootJsonFormat[FullJobConfiguration] {
    def write(x: FullJobConfiguration): JsValue = x match {
      case mistJobConfiguration: MistJobConfiguration => mistJobConfigurationFormat.write(mistJobConfiguration)
      case trainingJobConfiguration: TrainingJobConfiguration => trainingJobConfigurationFormat.write(trainingJobConfiguration)
      case servingJobConfiguration: ServingJobConfiguration => servingJobConfigurationFormat.write(servingJobConfiguration)
    }

    def read(v: JsValue): FullJobConfiguration = v.convertTo[MistJobConfiguration]
  }

  implicit val mistJobConfigurationFormat: RootJsonFormat[MistJobConfiguration] = jsonFormat6(MistJobConfiguration)
  implicit val mistJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedMistJobConfiguration] = jsonFormat3(RestificatedMistJobConfiguration)
  implicit val trainingJobConfigurationFormat: RootJsonFormat[TrainingJobConfiguration] = jsonFormat6(TrainingJobConfiguration)
  implicit val trainingJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedTrainingJobConfiguration] = jsonFormat3(RestificatedTrainingJobConfiguration)
  implicit val servingJobConfigurationFormat: RootJsonFormat[ServingJobConfiguration] = jsonFormat6(ServingJobConfiguration)
  implicit val servingJobRestificatedConfigurationFormat: RootJsonFormat[RestificatedServingJobConfiguration] = jsonFormat3(RestificatedServingJobConfiguration)

  implicit val jobResultFormat: RootJsonFormat[JobResult] = jsonFormat4(JobResult)

  sealed trait JobConfigError

  case class NoRouteError(reason: String) extends JobConfigError

  case class ConfigError(reason: String) extends JobConfigError

}
