package io.hydrosphere.mist.ml.classification

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{DataUtils, LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.tree.Node
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

object LocalDecisionTreeClassificationModel extends LocalTypedTransformer[DecisionTreeClassificationModel] {
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

  override def transformTyped(sparkTransformer: DecisionTreeClassificationModel, localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val method = classOf[DecisionTreeClassificationModel].getMethod("predict", classOf[Vector])
        method.setAccessible(true)
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data map { feature =>
          val vector: SparseVector = feature match {
            case v: SparseVector => v
            case v: SVector => DataUtils.mllibVectorToMlVector(v)
            case x => throw new IllegalArgumentException(s"$x is not a vector")
          }
          method.invoke(sparkTransformer, vector).asInstanceOf[Double]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
