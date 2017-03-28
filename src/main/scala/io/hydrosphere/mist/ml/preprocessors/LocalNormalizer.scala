package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.{Vector, Vectors}

class LocalNormalizer(override val sparkTransformer: Normalizer) extends LocalTransformer[Normalizer] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[Normalizer].getMethod("createTransformFunc")
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

object LocalNormalizer extends LocalModel[Normalizer] {
  override def load(metadata: Metadata, data: Map[String, Any]): Normalizer = {
    new Normalizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setP(metadata.paramMap("p").toString.toDouble)
  }

  override implicit def getTransformer(transformer: Normalizer): LocalTransformer[Normalizer] = new LocalNormalizer(transformer)
}
