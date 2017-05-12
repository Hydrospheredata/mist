package io.hydrosphere.mist.api.ml

import org.apache.spark.ml.PipelineModel

object PipelineLoader {
  def load(path: String): PipelineModel = ModelLoader.get(path)
}
