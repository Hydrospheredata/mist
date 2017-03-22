package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.SQLTransformer


object LocalSQLTransformer extends LocalModel[SQLTransformer] {
  override def load(metadata: Metadata, data: Map[String, Any]): SQLTransformer = {
    new SQLTransformer(metadata.uid).setStatement(metadata.paramMap("statement").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: SQLTransformer): LocalTransformer[SQLTransformer] = ???
}
