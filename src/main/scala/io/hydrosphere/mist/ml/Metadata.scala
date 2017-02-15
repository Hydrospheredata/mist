package io.hydrosphere.mist.ml

case class Metadata(
                     className: String,
                     timestamp: Long,
                     sparkVersion: String,
                     uid: String,
                     paramMap: Map[String, Any],
                     numFeatures: Option[Int],
                     numClasses: Option[Int]
                   )