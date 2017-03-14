package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.SQLTransformer


object LocalSQLTransformer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new SQLTransformer(metadata.uid).setStatement(metadata.paramMap("statement").asInstanceOf[String])
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
