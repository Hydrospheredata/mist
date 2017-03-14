package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Normalizer


object LocalNormalizer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Normalizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setP(metadata.paramMap("P").asInstanceOf[Double])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
