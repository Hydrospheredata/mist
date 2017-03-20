package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{DataUtils, LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


object LocalVectorIndexer extends LocalTypedTransformer[VectorIndexerModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val ctor = classOf[VectorIndexerModel].getDeclaredConstructor(
      classOf[String],
      classOf[Int],
      classOf[Map[Int, Map[Double, Int]]]
    )
    ctor.setAccessible(true)
    ctor
      .newInstance(
        metadata.uid,
        data("numFeatures").asInstanceOf[java.lang.Integer],
        DataUtils.flatConvertMap(data("categoryMaps").asInstanceOf[Map[String, Any]])
      )
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transformTyped(vecIndx: VectorIndexerModel, localData: LocalData): LocalData = {
    val transformFunc: Vector => Vector = {
      val sortedCatFeatureIndices = vecIndx.categoryMaps.keys.toArray.sorted
      val localVectorMap = vecIndx.categoryMaps
      val localNumFeatures = vecIndx.numFeatures
      val f: Vector => Vector = { (v: Vector) =>
        assert(v.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
          s" $vecIndx.numFeatures but found length ${v.size}")
        v match {
          case dv: DenseVector =>
            val tmpv = dv.copy
            localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
              tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
            }
            tmpv
          case sv: SparseVector =>
            // We use the fact that categorical value 0 is always mapped to index 0.
            val tmpv = sv.copy
            var catFeatureIdx = 0
            // index into sortedCatFeatureIndices
            var k = 0 // index into non-zero elements of sparse vector
            while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
              val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
              if (featureIndex < tmpv.indices(k)) {
                catFeatureIdx += 1
              } else if (featureIndex > tmpv.indices(k)) {
                k += 1
              } else {
                tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
                catFeatureIdx += 1
                k += 1
              }
            }
            tmpv
        }
      }
      f
    }
    localData.column(vecIndx.getInputCol) match {
      case Some(column) =>
        val newColumn = LocalDataColumn(vecIndx.getOutputCol, column.data map { data =>
          val vec = data.asInstanceOf[Vector]
          transformFunc(vec)
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
