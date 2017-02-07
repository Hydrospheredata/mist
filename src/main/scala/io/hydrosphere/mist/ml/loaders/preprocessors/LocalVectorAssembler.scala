package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler


object LocalVectorAssembler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new VectorAssembler(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    }
}
