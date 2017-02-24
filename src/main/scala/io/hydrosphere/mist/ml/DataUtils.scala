package io.hydrosphere.mist.ml

import java.util

import org.apache.spark.ml.linalg.{DenseVector, Matrices, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.{SparseVector => SVector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}


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

  def asBreeze(values: Array[Double]): BV[Double] = new BDV[Double](values)

  def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DenseVector(v.data)
        } else {
          new DenseVector(v.toArray)
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(v.length, v.index, v.data)
        } else {
          new SparseVector(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }

  /**
    * NOTE: for faster implementation use org.apache.spark.util.collection.OpenHashMap
    *
    * @param labels Array[String]
    * @return HashMap
    */
  def labelToIndex(labels: Array[String]): util.HashMap[String, Double] = {
    val n = labels.length
    val map = new util.HashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.put(labels(i), i)
      i += 1
    }
    map
  }
}
