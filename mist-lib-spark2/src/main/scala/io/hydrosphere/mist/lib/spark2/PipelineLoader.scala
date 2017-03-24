package io.hydrosphere.mist.lib.spark2

import org.apache.spark.ml.PipelineModel

object PipelineLoader {

  def load(path: String): PipelineModel = ml.ModelLoader.get(path)

}
