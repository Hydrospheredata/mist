package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.NGram


object LocalNGram extends LocalModel[NGram] {
  override def load(metadata: Metadata, data: Map[String, Any]): NGram = {
    new NGram(metadata.uid)
        .setN(metadata.paramMap("N").asInstanceOf[Int])
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: NGram): LocalTransformer[NGram] = ???
}
