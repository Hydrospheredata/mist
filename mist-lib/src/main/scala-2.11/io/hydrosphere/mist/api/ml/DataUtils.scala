package io.hydrosphere.mist.api.ml

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.ml.linalg.{DenseVector, Matrices, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.linalg.{SparseVector => SVector}

object DataUtils {
  implicit def mllibVectorToMlVector(v: SVector): SparseVector = new SparseVector(v.size, v.indices, v.values)
  implicit class KindaListOfDoubles(val list: List[Double]) {
    /**
      * This is workaround for current JSON serialization. It places Int's to List[Double] and that causes exceptions.
      * WARNING: this method is very heavy, use only when you are not sure if list is pure, e.g. read from JSON.
      *
      * @return
      */
    def forceDoubles: List[Double] = list.asInstanceOf[List[AnyVal]] map(_.toString.toDouble)
  }

  def constructMatrix(params: Map[String, Any]): Matrix = {
    val numRows = params("numRows").asInstanceOf[Int]
    val numCols = params("numCols").asInstanceOf[Int]
    val values = params("values").asInstanceOf[List[Double]].toArray

    if (params.contains("colPtrs")) {
      val colPtrs = params("colPtrs").asInstanceOf[Array[Int]]
      val rowIndices = params("rowIndices").asInstanceOf[Array[Int]]
      val matrix = Matrices.sparse(numRows, numCols, colPtrs, rowIndices, values)

      if (params.keySet.contains("isTransposed")) {
        val isTransposed = matrix.getClass.getDeclaredField("isTransposed")
        isTransposed.setAccessible(true)
        isTransposed.setBoolean(matrix, params("isTransposed").asInstanceOf[Boolean])
      }
      matrix
    } else {
      val matrix = Matrices.dense(numRows, numCols, values)

      if (params.keySet.contains("isTransposed")) {
        val isTransposed = matrix.getClass.getDeclaredField("isTransposed")
        isTransposed.setAccessible(true)
        isTransposed.setBoolean(matrix, params("isTransposed").asInstanceOf[Boolean])
      }
      matrix
    }
  }

  def constructVector(params: Map[String, Any]): Vector = {
    if (params.contains("size")) {
      Vectors.sparse(
        params("size").asInstanceOf[Int],
        params("indices").asInstanceOf[List[Int]].toArray[Int],
        params("values").asInstanceOf[List[Double]].toArray[Double]
      )
    } else {
      Vectors.dense(params("values").asInstanceOf[List[Double]].toArray[Double])
    }
  }

  def createNode(nodeId: Int, metadata: Metadata, treeData: Map[String, Any]): Node = {
    val nodeData = treeData(nodeId.toString).asInstanceOf[Map[String, Any]]
    val impurity = DataUtils.createImpurityCalculator(
      metadata.paramMap("impurity").asInstanceOf[String],
      nodeData("impurityStats").asInstanceOf[List[Double]].to[Array]
    )

    if (isInternalNode(nodeData)) {
      val ctor = classOf[InternalNode].getDeclaredConstructor(
        classOf[Double],
        classOf[Double],
        classOf[Double],
        classOf[Node],
        classOf[Node],
        classOf[Split],
        impurity.getClass.getSuperclass
      )
      ctor.newInstance(
        nodeData("prediction").asInstanceOf[java.lang.Double],
        nodeData("impurity").asInstanceOf[java.lang.Double],
        nodeData("gain").asInstanceOf[java.lang.Double],
        createNode(nodeData("leftChild").asInstanceOf[java.lang.Integer], metadata, treeData),
        createNode(nodeData("rightChild").asInstanceOf[java.lang.Integer], metadata, treeData),
        DataUtils.createSplit(nodeData("split").asInstanceOf[Map[String, Any]]),
        impurity
      )
    } else {
      val ctor = classOf[LeafNode].getDeclaredConstructor(
        classOf[Double],
        classOf[Double],
        impurity.getClass.getSuperclass
      )
      ctor.newInstance(
        nodeData("prediction").asInstanceOf[java.lang.Double],
        nodeData("impurity").asInstanceOf[java.lang.Double],
        impurity
      )
    }
  }

  def isInternalNode(nodeData: Map[String, Any]): Boolean =
    (nodeData("leftChild").asInstanceOf[java.lang.Integer] != -1) && (nodeData("rightChild").asInstanceOf[java.lang.Integer] != -1)


  def createImpurityCalculator(impurity: String, stats: Array[Double]): Object = {
    val className = impurity match {
      case "gini" => "org.apache.spark.mllib.tree.impurity.GiniCalculator"
      case "entropy" => "org.apache.spark.mllib.tree.impurity.EntropyCalculator"
      case "variance" => "org.apache.spark.mllib.tree.impurity.VarianceCalculator"
      case _ =>
        throw new IllegalArgumentException(s"ImpurityCalculator builder did not recognize impurity type: $impurity")
    }
    val ctor = Class.forName(className).getDeclaredConstructor(classOf[Array[Double]])
    ctor.setAccessible(true)
    ctor.newInstance(stats).asInstanceOf[Object]
  }

  def createSplit(data: Map[String, Any]): Split = {
    val cot = data("leftCategoriesOrThreshold").asInstanceOf[List[Double]]
    data("numCategories").toString.toInt match {
      case -1 =>
        val ctor = classOf[ContinuousSplit].getDeclaredConstructor(classOf[Int], classOf[Double])
        ctor.setAccessible(true)
        ctor.newInstance(
          data("featureIndex").asInstanceOf[java.lang.Integer],
          cot.head.asInstanceOf[java.lang.Double]
        )
      case x =>
        val ctor = classOf[CategoricalSplit].getDeclaredConstructor(classOf[Int], classOf[Array[Double]], classOf[Int])
        ctor.setAccessible(true)
        ctor.newInstance(
          data("featureIndex").asInstanceOf[java.lang.Integer],
          cot.to[Array],
          x.asInstanceOf[java.lang.Integer]
        )
    }
  }

  def kludgeForVectorIndexer(map: Map[String, Any]): Map[Int, Map[Double, Int]] = {
    map.map({ case (k, v) =>
      val key = k.toInt
      val value = v.asInstanceOf[Map[String, Int]].map(x => x._1.toDouble -> x._2)
      key -> value
    })
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
}
