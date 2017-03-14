package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.HashingTF

object LocalHashingTF extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new HashingTF(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setBinary(metadata.paramMap("binary").asInstanceOf[Boolean])
      .setNumFeatures(metadata.paramMap("numFeatures").asInstanceOf[Int])
  }
}
