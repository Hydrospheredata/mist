package io.hydrosphere.mist.lib.spark2.ml.regression

import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree.Node
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

class LocalDecisionTreeRegressionModel(override val sparkTransformer: DecisionTreeRegressionModel) extends LocalTransformer[DecisionTreeRegressionModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val method = classOf[DecisionTreeRegressionModel].getMethod("predict", classOf[Vector])
        method.setAccessible(true)
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data map { feature =>
          val vector = feature.asInstanceOf[Vector]
          method.invoke(sparkTransformer, vector).asInstanceOf[Double]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalDecisionTreeRegressionModel extends LocalModel[DecisionTreeRegressionModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): DecisionTreeRegressionModel = {
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

  override implicit def getTransformer(transformer: DecisionTreeRegressionModel): LocalTransformer[DecisionTreeRegressionModel] = new LocalDecisionTreeRegressionModel(transformer)
}
