package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.mllib.feature.{IDFModel => OldIDFModel}
import org.apache.spark.mllib.linalg.{DenseVector => OldDenseVector, SparseVector => OldSparseVector, Vector => OldVector, Vectors => OldVectors}

class LocalIDF(override val sparkTransformer: IDFModel) extends LocalTransformer[IDFModel] {
  override def transform(localData: LocalData): LocalData = {
    val idf = sparkTransformer.idf

    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newData = column.data.map(r => {
          val n = r.asInstanceOf[OldVector].size
          r match {
            case OldSparseVector(_, indices, values) =>
              val nnz = indices.length
              val newValues = new Array[Double](nnz)
              var k = 0
              while (k < nnz) {
                newValues(k) = values(k) * idf(indices(k))
                k += 1
              }
              OldVectors.sparse(n, indices, newValues)
            case OldDenseVector(values) =>
              val newValues = new Array[Double](n)
              var j = 0
              while (j < n) {
                newValues(j) = values(j) * idf(j)
                j += 1
              }
              OldVectors.dense(newValues)
            case other =>
              throw new UnsupportedOperationException(
                s"Only sparse and dense vectors are supported but got ${other.getClass}.")
          }
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalIDF extends LocalModel[IDFModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): IDFModel = {
    val idfValues = data("idf").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val vector = OldVectors.dense(idfValues)
    val oldIDFconstructor = classOf[OldIDFModel].getDeclaredConstructor(classOf[OldVector])
    oldIDFconstructor.setAccessible(true)
    val oldIDF = oldIDFconstructor.newInstance(vector)

    val const = classOf[IDFModel].getDeclaredConstructor(classOf[String], classOf[OldIDFModel])
    val idf = const.newInstance(metadata.uid, oldIDF)
    idf
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .set(idf.minDocFreq, metadata.paramMap("minDocFreq").asInstanceOf[Number].intValue())
  }

  override implicit def getTransformer(transformer: IDFModel): LocalTransformer[IDFModel] = new LocalIDF(transformer)
}
