package io.hydrosphere.mist.lib.spark2.ml.classification

import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrix, Vector}

object LocalNaiveBayes extends LocalModel[NaiveBayesModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): NaiveBayesModel = {
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

  override implicit def getTransformer(transformer: NaiveBayesModel): LocalTransformer[NaiveBayesModel] = ???
}
