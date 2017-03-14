package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.NGram


object LocalNGram extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new NGram(metadata.uid)
        .setN(metadata.paramMap("N").asInstanceOf[Int])
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
