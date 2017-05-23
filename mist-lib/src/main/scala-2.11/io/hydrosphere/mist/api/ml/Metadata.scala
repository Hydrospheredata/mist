package io.hydrosphere.mist.api.ml

case class Metadata(
  `class`: String,
  timestamp: Long,
  sparkVersion: String,
  uid: String,
  paramMap: Map[String, Any],
  numFeatures: Option[Int],
  numClasses: Option[Int],
  numTrees: Option[Int]
)

object Metadata {

  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats

  def fromJson(json: String): Metadata = {
    parse(json).extract[Metadata]
  }
}