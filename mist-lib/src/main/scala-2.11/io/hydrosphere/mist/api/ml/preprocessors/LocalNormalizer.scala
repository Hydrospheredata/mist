package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

class LocalNormalizer(override val sparkTransformer: Normalizer) extends LocalTransformer[Normalizer] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[Normalizer].getMethod("createTransformFunc")
        val newData = column.data.map(r => {
          val vector = r match {
            case x: List[Any] => Vectors.dense(x.map(_.toString.toDouble).toArray)
            case x: SparseVector => x
            case x: DenseVector => x
            case unknown =>
              throw new IllegalArgumentException(s"Unknown data type for LocalMaxAbsScaler: ${unknown.getClass}")
          }
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
