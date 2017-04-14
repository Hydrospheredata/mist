package io.hydrosphere.mist.lib.spark2.ml2

import org.apache.spark.ml.PipelineModel

object PipelineLoader {

  def load(path: String): PipelineModel = ModelLoader.get(path)

}
