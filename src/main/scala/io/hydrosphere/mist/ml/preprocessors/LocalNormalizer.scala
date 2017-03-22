package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Normalizer


object LocalNormalizer extends LocalModel[Normalizer] {
  override def load(metadata: Metadata, data: Map[String, Any]): Normalizer = {
    new Normalizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setP(metadata.paramMap("P").asInstanceOf[Double])
  }

  override implicit def getTransformer(transformer: Normalizer): LocalTransformer[Normalizer] = ???
}
