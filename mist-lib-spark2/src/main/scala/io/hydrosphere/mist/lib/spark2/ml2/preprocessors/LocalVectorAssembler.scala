package io.hydrosphere.mist.lib.spark2.ml2.preprocessors

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.feature.VectorAssembler


object LocalVectorAssembler extends LocalModel[VectorAssembler] {
  override def load(metadata: Metadata, data: Map[String, Any]): VectorAssembler = {
    new VectorAssembler(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    }

  override implicit def getTransformer(transformer: VectorAssembler): LocalTransformer[VectorAssembler] = ???
}
