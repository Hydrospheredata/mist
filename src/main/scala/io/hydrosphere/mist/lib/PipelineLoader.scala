package io.hydrosphere.mist.lib

import io.hydrosphere.mist.ml.ModelLoader
import org.apache.spark.ml.Transformer

object PipelineLoader {

  def load(path: String): Array[Transformer] = {
    ModelLoader.get(path)
  }

}
