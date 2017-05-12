package io.hydrosphere.mist.api.ml.regression

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegressionModel

class LocalLinearRegressionModel(override val sparkTransformer: LinearRegressionModel) extends LocalTransformer[LinearRegressionModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val predict = classOf[LinearRegressionModel].getMethod("predict", classOf[Vector])
        predict.setAccessible(true)
        val newCol = LocalDataColumn(sparkTransformer.getPredictionCol, column.data.map { data =>
          val vector = data.asInstanceOf[Vector]
          predict.invoke(sparkTransformer,vector).asInstanceOf[Double]
        })
        localData.withColumn(newCol)
      case None =>
        localData
    }
  }
}

object LocalLinearRegressionModel extends LocalModel[LinearRegressionModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): LinearRegressionModel = {
    val intercept = data("intercept").asInstanceOf[java.lang.Double]
    val coeffitientsMap = data("coefficients").asInstanceOf[Map[String, Any]]
    val coeffitients = DataUtils.constructVector(coeffitientsMap)

    val ctor = classOf[LinearRegressionModel].getConstructor(classOf[String], classOf[Vector], classOf[Double])
    val inst = ctor.newInstance(metadata.uid, coeffitients, intercept)
    inst
      .set(inst.featuresCol, metadata.paramMap("featuresCol").asInstanceOf[String])
      .set(inst.predictionCol, metadata.paramMap("predictionCol").asInstanceOf[String])
      .set(inst.labelCol, metadata.paramMap("labelCol").asInstanceOf[String])
      .set(inst.elasticNetParam, metadata.paramMap("elasticNetParam").toString.toDouble)
      // NOTE: introduced in spark 2.1 for reducing iterations for big datasets, e.g unnecessary for us
      //.set(inst.aggregationDepth, metadata.paramMap("aggregationDepth").asInstanceOf[Int])
      .set(inst.maxIter, metadata.paramMap("maxIter").asInstanceOf[Number].intValue())
      .set(inst.regParam, metadata.paramMap("regParam").toString.toDouble)
      .set(inst.solver, metadata.paramMap("solver").asInstanceOf[String])
      .set(inst.tol, metadata.paramMap("tol").toString.toDouble)
      .set(inst.standardization, metadata.paramMap("standardization").asInstanceOf[Boolean])
      .set(inst.fitIntercept, metadata.paramMap("fitIntercept").asInstanceOf[Boolean])
  }

  override implicit def getTransformer(transformer: LinearRegressionModel): LocalTransformer[LinearRegressionModel] = new LocalLinearRegressionModel(transformer)
}
