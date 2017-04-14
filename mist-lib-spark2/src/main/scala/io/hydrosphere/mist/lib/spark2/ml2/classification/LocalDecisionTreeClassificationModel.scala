package io.hydrosphere.mist.lib.spark2.ml2.classification

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.tree.Node
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

class LocalDecisionTreeClassificationModel(override val sparkTransformer: DecisionTreeClassificationModel) extends LocalTransformer[DecisionTreeClassificationModel] {
  override def transform(localData: LocalData): LocalData = {
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
    inst.setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: DecisionTreeClassificationModel): LocalTransformer[DecisionTreeClassificationModel] = new LocalDecisionTreeClassificationModel(transformer)
}
