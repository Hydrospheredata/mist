package io.hydrosphere.mist.lib.spark2.ml2.preprocessors

import io.hydrosphere.mist.lib.spark2.ml2._
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
