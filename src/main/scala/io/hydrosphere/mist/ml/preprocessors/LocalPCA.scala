package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Vectors}
import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import org.apache.spark.ml.Transformer
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

object LocalPCA extends LocalTypedTransformer[PCAModel] {
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

  override def transformTyped(pca: PCAModel, localData: LocalData): LocalData = {
    localData.column(pca.getInputCol) match {
      case Some(column) =>
        val newData = column.data.map(r => {
          val pc = OldMatrices.fromML(pca.pc).asInstanceOf[OldDenseMatrix]
          val vec: List[Double] = r match {
            case d: List[Any @unchecked] =>
              val l: List[Double] = d map (_.toString.toDouble)
              l
            case d => throw new IllegalArgumentException(s"Unknown data type for LocalPCA: $d")
          }
          val vector = OldVectors.dense(vec.toArray)
          pc.transpose.multiply(vector)
        })
        localData.withColumn(LocalDataColumn(pca.getOutputCol, newData))
      case None => localData
    }
  }
}
