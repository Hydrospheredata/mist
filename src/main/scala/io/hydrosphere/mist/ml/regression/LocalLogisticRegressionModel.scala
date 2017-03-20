package io.hydrosphere.mist.ml.regression

import java.lang.Boolean

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.{Matrix, SparseMatrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{SparseVector => SVector}

object LocalLogisticRegressionModel extends LocalTypedTransformer[LogisticRegressionModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    if (data.contains("coefficients")) {
      // Spark 2.0
      val constructor = classOf[LogisticRegressionModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Double])
      constructor.setAccessible(true)
      val coefficientsParams = data("coefficients").asInstanceOf[Map[String, Any]]
      val coefficients = Vectors.sparse(
        coefficientsParams("size").asInstanceOf[Int],
        coefficientsParams("indices").asInstanceOf[List[Int]].toArray[Int],
        coefficientsParams("values").asInstanceOf[List[Double]].toArray[Double]
      )
      constructor
        .newInstance(metadata.uid, coefficients, data("intercept").asInstanceOf[java.lang.Double])
        .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
        .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
        .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
        .setThreshold(metadata.paramMap("threshold").asInstanceOf[Double])
    } else if (data.contains("coefficientMatrix")) {
      // Spark 2.1
      val constructor = classOf[LogisticRegressionModel].getDeclaredConstructor(classOf[String], classOf[Matrix], classOf[Vector], classOf[Int], java.lang.Boolean.TYPE)
      constructor.setAccessible(true)
      val coefficientMatrixParams = data("coefficientMatrix").asInstanceOf[Map[String, Any]]
      val coefficientMatrix = new SparseMatrix(
        coefficientMatrixParams("numRows").asInstanceOf[Int],
        coefficientMatrixParams("numCols").asInstanceOf[Int],
        coefficientMatrixParams("colPtrs").asInstanceOf[List[Int]].toArray[Int],
        coefficientMatrixParams("rowIndices").asInstanceOf[List[Int]].toArray[Int],
        coefficientMatrixParams("values").asInstanceOf[List[Double]].toArray[Double],
        coefficientMatrixParams("isTransposed").asInstanceOf[Boolean]
      )
      val interceptVectorParams = data("interceptVector").asInstanceOf[Map[String, Any]]
      val interceptVector = Vectors.dense(interceptVectorParams("values").asInstanceOf[List[Double]].toArray[Double])
      constructor
        .newInstance(metadata.uid, coefficientMatrix, interceptVector, new Integer(data("numFeatures").asInstanceOf[Int]), new Boolean(data("isMultinomial").asInstanceOf[Boolean]))
        .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
        .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
        .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
        .setThreshold(metadata.paramMap("threshold").asInstanceOf[Double])
    } else {
      throw new Exception("Unknown LogisticRegressionModel implementation")
    }
  }

  override def transformTyped(logisticRegression: LogisticRegressionModel, localData: LocalData): LocalData = {
    import io.hydrosphere.mist.ml.DataUtils._

    localData.column(logisticRegression.getFeaturesCol) match {
      case Some(column) =>
        var newData = localData
        val predict = classOf[LogisticRegressionModel].getMethod("predict", classOf[Vector])
        if (logisticRegression.getPredictionCol.nonEmpty) {
          val newColumn = LocalDataColumn(logisticRegression.getPredictionCol, column.data.map(m => {
            val vector: SparseVector = m.asInstanceOf[SVector]
            predict.invoke(logisticRegression, vector)
          }))
          newData = newData.withColumn(newColumn)
        }
        val predictRaw = classOf[LogisticRegressionModel].getMethod("predictRaw", classOf[Vector])
        if (logisticRegression.getRawPredictionCol.nonEmpty) {
          val newColumn = LocalDataColumn(logisticRegression.getRawPredictionCol, column.data.map(m => {
            val vector: SparseVector = m.asInstanceOf[SVector]
            predictRaw.invoke(logisticRegression, vector)
          }))
          newData = newData.withColumn(newColumn)
        }
        val predictProbability = classOf[LogisticRegressionModel].getMethod("predictProbability", classOf[AnyRef])
        if (logisticRegression.getProbabilityCol.nonEmpty) {
          val newColumn = LocalDataColumn(logisticRegression.getProbabilityCol, column.data.map(m => {
            val vector: SparseVector = m.asInstanceOf[SVector]
            predictProbability.invoke(logisticRegression, vector)
          }))
          newData = newData.withColumn(newColumn)
        }
        newData
      case None => localData
    }
  }
}
