package io.hydrosphere.mist.ml

case class Metadata(
                     className: String,
                     timestamp: Long,
                     sparkVersion: String,
                     uid: String,
                     paramMap: Map[String, Any],
                     numFeatures: Option[Int],
                     numClasses: Option[Int],
                     numTrees: Option[Int]
                   )

object Metadata {
  def fromMap(arg: Map[String, Any]): Metadata = {
    Metadata(
      arg("class").asInstanceOf[String],
      arg("timestamp").asInstanceOf[Long],
      arg("sparkVersion").asInstanceOf[String],
      arg("uid").asInstanceOf[String],
      arg("paramMap").asInstanceOf[Map[String, Any]],
      arg("numFeatures").asInstanceOf[Option[Int]],
      arg("numClasses").asInstanceOf[Option[Int]],
      arg("numTrees").asInstanceOf[Option[Int]]
    )
  }
}