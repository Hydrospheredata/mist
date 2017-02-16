package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.ml.Metadata
import spray.json.RootJsonFormat

private[mist] trait ModelMetadataJsonSerialization extends AnyJsonFormatSupport {

  implicit val stageMetadataSerialization: RootJsonFormat[Metadata] = jsonFormat(Metadata.apply, "class", "timestamp", "sparkVersion", "uid", "paramMap", "numFeatures", "numClasses")
  
}
