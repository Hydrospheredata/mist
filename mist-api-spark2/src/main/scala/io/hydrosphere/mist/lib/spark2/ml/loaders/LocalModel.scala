package io.hydrosphere.mist.lib.spark2.ml.loaders

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import org.apache.spark.ml.Transformer

trait LocalModel {
  def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer
}
