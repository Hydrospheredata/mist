package io.hydrosphere.mist.lib.spark2.ml.preprocessors

import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.Vectors

class LocalOneHotEncoder(override val sparkTransformer: OneHotEncoder) extends LocalTransformer[OneHotEncoder] {
  override def transform(localData: LocalData): LocalData = {
    val oneValue = Array(1.0)
    val emptyValues = Array.empty[Double]
    val emptyIndices = Array.empty[Int]

    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val col = column.data.asInstanceOf[List[Double]]
        col.foreach(x =>
          assert(x >= 0.0 && x == x.toInt,
            s"Values from column ${sparkTransformer.getInputCol} must be indices, but got $x.")
        )

        val size = col.max.toInt
        val newData = col.map(r => {
          if (r < size) {
            Vectors.sparse(size, Array(r.toInt), oneValue)
          } else {
            Vectors.sparse(size, emptyIndices, emptyValues)
          }
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalOneHotEncoder extends LocalModel[OneHotEncoder] {
  override def load(metadata: Metadata, data: Map[String, Any]): OneHotEncoder = {
    var ohe = new OneHotEncoder(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("dropLast").foreach{ x => ohe = ohe.setDropLast(x.asInstanceOf[Boolean])}

    ohe
  }

  override implicit def getTransformer(transformer: OneHotEncoder): LocalTransformer[OneHotEncoder] = new LocalOneHotEncoder(transformer)
}
