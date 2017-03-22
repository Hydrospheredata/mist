package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorAssembler


object LocalVectorAssembler extends LocalModel[VectorAssembler] {
  override def load(metadata: Metadata, data: Map[String, Any]): VectorAssembler = {
    new VectorAssembler(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    }

  override implicit def getTransformer(transformer: VectorAssembler): LocalTransformer[VectorAssembler] = ???
}
