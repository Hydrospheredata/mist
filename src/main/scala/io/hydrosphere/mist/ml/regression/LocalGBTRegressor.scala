package io.hydrosphere.mist.ml.regression

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{DataUtils, LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tree.Node

class LocalGBTRegressor(override val sparkTransformer: GBTRegressionModel)  extends LocalTransformer[GBTRegressionModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val method = classOf[GBTRegressionModel].getMethod("predict", classOf[Vector])
        method.setAccessible(true)
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data map { feature =>
          val vector: Vector = feature match {
            case x: Map[String, Any] =>
              x.get("type").get match {
                case 0 =>
                  val indices = x.getOrElse("indices", List()).asInstanceOf[List[Int]].toArray
                  val values = x.getOrElse("values", List()).asInstanceOf[List[Any]].map(_.toString.toDouble).toArray
                  Vectors.sparse(
                    x.getOrElse("size", 0).asInstanceOf[Int],
                    indices,
                    values
                  )
                case _ => Vectors.dense(x.getOrElse("values", List()).asInstanceOf[List[Any]].map(_.toString.toDouble).toArray)
              }
            case x: Vector => x
          }
          method.invoke(sparkTransformer, vector).asInstanceOf[Double]
        })
        localData.withColumn(newColumn)
      case None =>
        localData
    }
  }
}

object LocalGBTRegressor extends LocalModel[GBTRegressionModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): GBTRegressionModel = {
    val weights = metadata.paramMap("treesMetadata").asInstanceOf[Map[String, Any]] map { x =>
      x._2.asInstanceOf[Map[String, Any]].get("weights").get.toString.toDouble
    }

    val trees = metadata.paramMap("treesMetadata").asInstanceOf[Map[String, Any]] zip data map { x =>
      val treeID = x._1._1
      val meta = x._1._2.asInstanceOf[Map[String, Any]].get("metadata").get.asInstanceOf[Metadata]
      val weight = x._1._2.asInstanceOf[Map[String, Any]].get("weights").get.toString.toDouble
      val treeData =  x._2._2.asInstanceOf[Map[String, Any]]

      createTree(treeID.toString, meta, treeData)
    }

    val parent = new GBTRegressor()
      .setMaxIter(metadata.paramMap("maxIter").asInstanceOf[Int])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setLabelCol(metadata.paramMap("labelCol").asInstanceOf[String])
      .setSeed(metadata.paramMap("seed").toString.toLong)
      .setStepSize(metadata.paramMap("stepSize").toString.toDouble)
      .setSubsamplingRate(metadata.paramMap("subsamplingRate").toString.toDouble)
      .setImpurity(metadata.paramMap("impurity").asInstanceOf[String])
      .setMaxDepth(metadata.paramMap("maxDepth").asInstanceOf[Int])
      .setMinInstancesPerNode(metadata.paramMap("minInstancesPerNode").asInstanceOf[Int])
      .setCheckpointInterval(metadata.paramMap("checkpointInterval").asInstanceOf[Int])
      .setMinInfoGain(metadata.paramMap("minInfoGain").toString.toDouble)
      .setCacheNodeIds(metadata.paramMap("cacheNodeIds").asInstanceOf[Boolean])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setMaxMemoryInMB(metadata.paramMap("maxMemoryInMB").asInstanceOf[Int])
      .setMaxBins(metadata.paramMap("maxBins").asInstanceOf[Int])
      .setLossType(metadata.paramMap("lossType").asInstanceOf[String])

    val numFeatures: Int = metadata.numFeatures.getOrElse(0)

    val cstr = classOf[GBTRegressionModel].getDeclaredConstructor(
      classOf[String],
      classOf[Array[DecisionTreeRegressionModel]],
      classOf[Array[Double]],
      classOf[Int]
    )
    cstr.setAccessible(true)
    cstr.newInstance(
      metadata.uid,
      trees.toArray,
      weights.toArray,
      new java.lang.Integer(numFeatures)
    )
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setParent(parent)
  }

  def createTree(uid: String, metadata: Metadata, data: Map[String, Any]): DecisionTreeRegressionModel = {
    val ctor = classOf[DecisionTreeRegressionModel].getDeclaredConstructor(classOf[String], classOf[Node], classOf[Int])
    ctor.setAccessible(true)
    val inst = ctor.newInstance(
      uid,
      DataUtils.createNode(0, metadata, data),
      metadata.numFeatures.get.asInstanceOf[java.lang.Integer]
    )
    inst.setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: GBTRegressionModel): LocalTransformer[GBTRegressionModel] = new LocalGBTRegressor(transformer)
}