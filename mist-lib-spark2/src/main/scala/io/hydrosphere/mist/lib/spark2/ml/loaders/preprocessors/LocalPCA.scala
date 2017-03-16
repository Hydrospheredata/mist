package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Vectors}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix}


object LocalPCA extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[PCAModel].getDeclaredConstructor(classOf[String], classOf[DenseMatrix], classOf[DenseVector])
    constructor.setAccessible(true)
    if (data.contains("explainedVariance")) {
      // NOTE: Spark >= 2
      val numRows = data("pc").asInstanceOf[Map[String, Any]].getOrElse("numRows", 0).asInstanceOf[Int]
      val numCols = data("pc").asInstanceOf[Map[String, Any]].getOrElse("numCols", 0).asInstanceOf[Int]
      val pcValues = data("pc").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
      val pc = new DenseMatrix(numRows, numCols, pcValues)

      val evValues = data("explainedVariance").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
      val explainedVariance = new DenseVector(evValues)
      constructor
        .newInstance(metadata.uid, pc, explainedVariance)
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    } else {
      // NOTE: Spark < 2
      val numRows = data("pc").asInstanceOf[Map[String, Any]].getOrElse("numRows", 0).asInstanceOf[Int]
      val numCols = data("pc").asInstanceOf[Map[String, Any]].getOrElse("numCols", 0).asInstanceOf[Int]
      val pcValues = data("pc").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray

      val pc = new OldDenseMatrix(numRows, numCols, pcValues)
      constructor
        .newInstance(metadata.uid, pc.asML, Vectors.dense(Array.empty[Double]).asInstanceOf[DenseVector])
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    }
  }
}
