package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vector


object LocalElementwiseProduct extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new ElementwiseProduct(metadata.uid)
      .setScalingVec(metadata.paramMap("scalingVec").asInstanceOf[Vector])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
