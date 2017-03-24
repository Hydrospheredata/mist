package io.hydrosphere.mist.lib.spark2.ml.loaders.classification

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LocalPerceptron extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MultilayerPerceptronClassificationModel = {
    val constructor = classOf[MultilayerPerceptronClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[Map[String, Any]]("values").asInstanceOf[List[Double]].toArray))
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
