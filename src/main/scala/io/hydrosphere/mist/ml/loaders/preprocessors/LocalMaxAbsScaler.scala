package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector


object LocalMaxAbsScaler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[MaxAbsScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, data("maxAbs").asInstanceOf[Vector])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
