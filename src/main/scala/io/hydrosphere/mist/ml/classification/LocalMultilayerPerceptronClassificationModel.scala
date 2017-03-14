package io.hydrosphere.mist.ml.classification

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LocalMultilayerPerceptronClassificationModel extends LocalTypedTransformer[MultilayerPerceptronClassificationModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MultilayerPerceptronClassificationModel = {
    val constructor = classOf[MultilayerPerceptronClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[Map[String, Any]]("values").asInstanceOf[List[Double]].toArray))
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }

  override def transformTyped(perceptron: MultilayerPerceptronClassificationModel, localData: LocalData): LocalData = {
    localData.column(perceptron.getFeaturesCol) match {
      case Some(column) =>
        val method = classOf[MultilayerPerceptronClassificationModel].getMethod("predict", classOf[Vector])
        method.setAccessible(true)
        val newColumn = LocalDataColumn(perceptron.getPredictionCol, column.data map { feature =>
          method.invoke(perceptron, feature.asInstanceOf[Vector]).asInstanceOf[Double]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
