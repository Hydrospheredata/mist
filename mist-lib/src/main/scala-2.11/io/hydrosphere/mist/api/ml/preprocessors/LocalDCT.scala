package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.DCT
import org.apache.spark.ml.linalg.{Vector, Vectors}

class LocalDCT(override val sparkTransformer: DCT) extends LocalTransformer[DCT] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[DCT].getMethod("createTransformFunc")
        val newData = column.data.map(r => {
          val row = r.asInstanceOf[List[Any]].map(_.toString.toDouble).toArray
          val vector: Vector = Vectors.dense(row)
          method.invoke(sparkTransformer).asInstanceOf[Vector => Vector](vector)
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalDCT extends LocalModel[DCT] {
  override def load(metadata: Metadata, data: Map[String, Any]): DCT = {
    new DCT(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setInverse(metadata.paramMap("inverse").asInstanceOf[Boolean])
  }

  override implicit def getTransformer(transformer: DCT): LocalTransformer[DCT] = new LocalDCT(transformer)
}
