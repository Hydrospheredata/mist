package io.hydrosphere.mist.lib.spark2.ml.loaders.regression

import io.hydrosphere.mist.lib.spark2.ml.{Metadata, DataUtils}
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree.Node

object LocalDecisionTreeRegressionModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): DecisionTreeRegressionModel = {
    createTree(metadata, data)
  }

  def createTree(metadata: Metadata, data: Map[String, Any]): DecisionTreeRegressionModel = {
    val ctor = classOf[DecisionTreeRegressionModel].getDeclaredConstructor(classOf[String], classOf[Node], classOf[Int])
    ctor.setAccessible(true)
    val inst = ctor.newInstance(
      metadata.uid,
      DataUtils.createNode(0, metadata, data),
      metadata.numFeatures.get.asInstanceOf[java.lang.Integer]
    )
    inst.setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
