package io.hydrosphere.mist.lib.spark2.ml.loaders.classification

import io.hydrosphere.mist.lib.spark2.ml.{Metadata, DataUtils}
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.tree.Node

object LocalDecisionTreeClassificationModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
    createTree(metadata, data)
  }

  def createTree(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
    val ctor = classOf[DecisionTreeClassificationModel].getDeclaredConstructor(classOf[String], classOf[Node], classOf[Int], classOf[Int])
    ctor.setAccessible(true)
    val inst = ctor.newInstance(
      metadata.uid,
      DataUtils.createNode(0, metadata, data),
      metadata.numFeatures.get.asInstanceOf[java.lang.Integer],
      metadata.numClasses.get.asInstanceOf[java.lang.Integer]
    )
    inst.setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
  }
}
