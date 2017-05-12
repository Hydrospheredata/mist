package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.{Vector, Vectors}

class LocalPolynomialExpansion(override val sparkTransformer: PolynomialExpansion) extends LocalTransformer[PolynomialExpansion] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[PolynomialExpansion].getMethod("createTransformFunc")
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

object LocalPolynomialExpansion extends LocalModel[PolynomialExpansion] {
  override def load(metadata: Metadata, data: Map[String, Any]): PolynomialExpansion = {
    new PolynomialExpansion(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setDegree(metadata.paramMap("degree").asInstanceOf[Number].intValue())
  }

  override implicit def getTransformer(transformer: PolynomialExpansion): LocalTransformer[PolynomialExpansion] = new LocalPolynomialExpansion(transformer)
}
