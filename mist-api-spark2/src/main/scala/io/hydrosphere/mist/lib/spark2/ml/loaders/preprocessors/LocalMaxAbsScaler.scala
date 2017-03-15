package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{Vector, DenseVector}


object LocalMaxAbsScaler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val maxAbsList = data("maxAbs").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val maxAbs = new DenseVector(maxAbsList)

    val constructor = classOf[MaxAbsScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, maxAbs)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
