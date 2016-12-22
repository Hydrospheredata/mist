package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.ml.Metadata
import spray.json.RootJsonFormat

private[mist] trait ModelMetadataJsonSerialization extends JsonFormatSupport {

  implicit val stageMetadataSerialization: RootJsonFormat[Metadata] = jsonFormat5(Metadata)
  
}
