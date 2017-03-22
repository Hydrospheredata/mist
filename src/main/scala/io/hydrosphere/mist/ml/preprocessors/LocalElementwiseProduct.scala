package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vector


object LocalElementwiseProduct extends LocalModel[ElementwiseProduct] {
  override def load(metadata: Metadata, data: Map[String, Any]): ElementwiseProduct = {
    new ElementwiseProduct(metadata.uid)
      .setScalingVec(metadata.paramMap("scalingVec").asInstanceOf[Vector])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: ElementwiseProduct): LocalTransformer[ElementwiseProduct] = ???
}
