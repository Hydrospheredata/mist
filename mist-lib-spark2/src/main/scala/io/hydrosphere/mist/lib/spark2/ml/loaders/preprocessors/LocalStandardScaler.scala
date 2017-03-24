package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseVector, Vector}


object LocalStandardScaler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[StandardScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Vector])
    constructor.setAccessible(true)

    val stdVals = data("std").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
    val std = new DenseVector(stdVals)

    val meanVals = data("mean").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
    val mean = new DenseVector(meanVals)
    constructor
      .newInstance(metadata.uid, std, mean)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
