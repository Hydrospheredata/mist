package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Interaction


object LocalInteraction extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Interaction(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
