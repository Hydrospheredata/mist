package io.hydrosphere.mist.api.ml.classification

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.tree.Node

class LocalDecisionTreeClassificationModel(override val sparkTransformer: DecisionTreeClassificationModel) extends LocalTransformer[DecisionTreeClassificationModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val method = classOf[DecisionTreeClassificationModel].getMethod("predict", classOf[Vector])
        method.setAccessible(true)
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data.map(f => Vectors.dense(f.asInstanceOf[Array[Double]])).map { vector =>
          method.invoke(sparkTransformer, vector).asInstanceOf[Double]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalDecisionTreeClassificationModel extends LocalModel[DecisionTreeClassificationModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): DecisionTreeClassificationModel = {
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
    inst
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setRawPredictionCol(metadata.paramMap("rawPredictionCol").asInstanceOf[String])
    inst
      .set(inst.seed, metadata.paramMap("seed").toString.toLong)
      .set(inst.cacheNodeIds, metadata.paramMap("cacheNodeIds").toString.toBoolean)
      .set(inst.maxDepth, metadata.paramMap("maxDepth").toString.toInt)
      .set(inst.labelCol, metadata.paramMap("labelCol").toString)
      .set(inst.minInfoGain, metadata.paramMap("minInfoGain").toString.toDouble)
      .set(inst.checkpointInterval, metadata.paramMap("checkpointInterval").toString.toInt)
      .set(inst.minInstancesPerNode, metadata.paramMap("minInstancesPerNode").toString.toInt)
      .set(inst.maxMemoryInMB, metadata.paramMap("maxMemoryInMB").toString.toInt)
      .set(inst.maxBins, metadata.paramMap("maxBins").toString.toInt)
      .set(inst.impurity, metadata.paramMap("impurity").toString)
  }

  override implicit def getTransformer(transformer: DecisionTreeClassificationModel): LocalTransformer[DecisionTreeClassificationModel] = new LocalDecisionTreeClassificationModel(transformer)
}
