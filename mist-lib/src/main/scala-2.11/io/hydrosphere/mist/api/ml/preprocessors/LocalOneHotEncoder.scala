package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg.Vectors

class LocalOneHotEncoder(override val sparkTransformer: OneHotEncoder) extends LocalTransformer[OneHotEncoder] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val col = column.data.asInstanceOf[List[Double]]
        col.foreach(x =>
          assert(x >= 0.0 && x == x.toInt,
            s"Values from column ${sparkTransformer.getInputCol} must be indices, but got $x.")
        )

        val size = col.max.toInt
        val newData = col.map(r => {
          val res = Array.fill(size){0.0}
          if (r < size) {
            res.update(r.toInt, 1.0)
          }
          res
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
