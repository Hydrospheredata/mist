package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.DCT

object LocalDCT extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new DCT(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setInverse(metadata.paramMap("inverse").asInstanceOf[Boolean])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
