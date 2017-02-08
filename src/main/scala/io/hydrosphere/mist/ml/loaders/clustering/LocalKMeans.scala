package io.hydrosphere.mist.ml.loaders.clustering

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg.{Matrix, Vector}

//TODO
object LocalKMeans extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): KMeansModel = {
    val constructor = classOf[KMeansModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Matrix])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, metadata.paramMap("pi"), metadata.paramMap("theta"))
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
