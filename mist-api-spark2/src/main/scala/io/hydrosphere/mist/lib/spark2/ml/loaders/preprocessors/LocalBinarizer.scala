package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Binarizer


object LocalBinarizer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Binarizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setThreshold(metadata.paramMap("threshold").toString.toDouble)
  }
}
