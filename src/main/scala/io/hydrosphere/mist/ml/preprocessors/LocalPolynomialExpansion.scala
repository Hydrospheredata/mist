package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.PolynomialExpansion


object LocalPolynomialExpansion extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new PolynomialExpansion(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setDegree(metadata.paramMap("degree").asInstanceOf[Int])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
