package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.Transformer

trait LocalModel {
  def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer
}
