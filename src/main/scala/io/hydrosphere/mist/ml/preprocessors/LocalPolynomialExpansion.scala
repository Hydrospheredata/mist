package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.PolynomialExpansion


object LocalPolynomialExpansion extends LocalModel[PolynomialExpansion] {
  override def load(metadata: Metadata, data: Map[String, Any]): PolynomialExpansion = {
    new PolynomialExpansion(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setDegree(metadata.paramMap("degree").asInstanceOf[Int])
  }

  override implicit def getTransformer(transformer: PolynomialExpansion): LocalTransformer[PolynomialExpansion] = ???
}
