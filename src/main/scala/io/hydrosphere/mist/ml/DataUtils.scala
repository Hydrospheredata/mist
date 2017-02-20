package io.hydrosphere.mist.ml

import org.apache.spark.ml.linalg.{Matrices, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{SparseVector => SVector}
/**
  * Created by Bulat on 09.02.2017.
  */
object DataUtils {
  implicit def mllibVectorToMlVector(v: SVector): SparseVector = new SparseVector(v.size, v.indices, v.values)

  def constructMatrix(params: Map[String, Any]): Matrix = {
    val numRows = params("numRows").asInstanceOf[Int]
    val numCols = params("numCols").asInstanceOf[Int]
    val values = params("values").asInstanceOf[Array[Double]]

    if(params.contains("colPtrs")) {
      val colPtrs = params("colPtrs").asInstanceOf[Array[Int]]
      val rowIndices = params("rowIndices").asInstanceOf[Array[Int]]
      Matrices.sparse(numRows, numCols, colPtrs, rowIndices, values)
    } else {
      Matrices.dense(numRows, numCols, values)
    }
  }

  def constructVector(params: Map[String, Any]): Vector = {
    Vectors.sparse(
      params("size").asInstanceOf[Int],
      params("indices").asInstanceOf[List[Int]].toArray[Int],
      params("values").asInstanceOf[List[Double]].toArray[Double]
    )
  }
}
