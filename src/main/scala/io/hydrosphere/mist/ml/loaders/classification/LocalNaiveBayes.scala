package io.hydrosphere.mist.ml.loaders.classification

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrix, Vector}

object LocalNaiveBayes extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): NaiveBayesModel = {
    val constructor = classOf[NaiveBayesModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Matrix])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, metadata.paramMap("pi"), metadata.paramMap("theta"))
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setThresholds(metadata.paramMap("thresholds").asInstanceOf[List[Double]].toArray[Double])
  }
}
