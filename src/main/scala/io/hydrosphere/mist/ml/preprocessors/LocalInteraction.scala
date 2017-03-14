package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Interaction


object LocalInteraction extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Interaction(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
