package io.hydrosphere.mist.api.ml.preprocessors

import java.lang.reflect.InvocationTargetException

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

class LocalVectorIndexerModel(override val sparkTransformer: VectorIndexerModel) extends LocalTransformer[VectorIndexerModel] {
  override def transform(localData: LocalData): LocalData = {
    val transformFunc: Vector => Vector = {
      val sortedCatFeatureIndices = sparkTransformer.categoryMaps.keys.toArray.sorted
      val localVectorMap = sparkTransformer.categoryMaps
      val localNumFeatures = sparkTransformer.numFeatures
      val f: Vector => Vector = { (v: Vector) =>
        assert(v.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
          s" $sparkTransformer.numFeatures but found length ${v.size}")
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
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newColumn = LocalDataColumn(sparkTransformer.getOutputCol, column.data.map(f => Vectors.dense(f.asInstanceOf[Array[Double]])) map { data =>
          transformFunc(data).toDense.values
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalVectorIndexerModel extends LocalModel[VectorIndexerModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): VectorIndexerModel = {
    val ctor = classOf[VectorIndexerModel].getDeclaredConstructor(
      classOf[String],
      classOf[Int],
      classOf[Map[Int, Map[Double, Int]]]
    )
    ctor.setAccessible(true)
    val categoryMaps = DataUtils.kludgeForVectorIndexer(data("categoryMaps").asInstanceOf[Map[String, Any]])
    try {
      ctor
        .newInstance(
          metadata.uid,
          data("numFeatures").asInstanceOf[java.lang.Integer],
          categoryMaps
        )
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    } catch {
      case e: InvocationTargetException => throw e.getTargetException
      case e: Throwable => throw e
    }
  }

  override implicit def getTransformer(transformer: VectorIndexerModel): LocalTransformer[VectorIndexerModel] = new LocalVectorIndexerModel(transformer)
}
