package io.hydrosphere.mist.ml.loaders.classification

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}

object LocalRandomForestClassificationModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): RandomForestClassificationModel = {
    val constructor = classOf[RandomForestClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[DecisionTreeClassificationModel]], classOf[Int], classOf[Int])
    constructor.setAccessible(true)
    val treesMetadata = metadata.paramMap("trees").asInstanceOf[List[Metadata]]
    val trees = treesMetadata map LocalDecisionTreeClassificationModel.createTree
    constructor
      .newInstance(metadata.uid, trees, metadata.paramMap("numFeatures").asInstanceOf[Int], metadata.paramMap("numClasses").asInstanceOf[Int])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setThresholds(metadata.paramMap("thresholds").asInstanceOf[List[Double]].toArray[Double])
  }
}
