package io.hydrosphere.mist.ml.loaders.classification

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.tree.Node

object LocalDecisionTreeClassificationModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
    createTree(metadata)
  }
  // TODO root node asInstanceOf[Node]
  def createTree(metadata: Metadata): DecisionTreeClassificationModel = {
    val constructor = classOf[DecisionTreeClassificationModel].getDeclaredConstructor(classOf[String], classOf[Node], classOf[Int], classOf[Int])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, metadata.paramMap("rootNode").asInstanceOf[Node], metadata.paramMap("numFeatures").asInstanceOf[Int], metadata.paramMap("numClasses").asInstanceOf[Int])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setThresholds(metadata.paramMap("thresholds").asInstanceOf[List[Double]].toArray[Double])
  }
}
