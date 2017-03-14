package io.hydrosphere.mist.ml.classification

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalTypedTransformer, Metadata}
import io.hydrosphere.mist.utils.DataUtils
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrix, Vector}

object LocalNaiveBayes extends LocalTypedTransformer[NaiveBayesModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): NaiveBayesModel = {
    val constructor = classOf[NaiveBayesModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Matrix])
    constructor.setAccessible(true)
    val matrixMetadata = metadata.paramMap("theta").asInstanceOf[Map[String, Any]]
    val matrix = DataUtils.constructMatrix(matrixMetadata)
    constructor
      .newInstance(metadata.uid, metadata.paramMap("pi").asInstanceOf[Array[Double]], matrix)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setThresholds(metadata.paramMap("thresholds").asInstanceOf[List[Double]].toArray[Double])
  }

  override def transformTyped(transformer: NaiveBayesModel, localData: LocalData): LocalData = ???
}
