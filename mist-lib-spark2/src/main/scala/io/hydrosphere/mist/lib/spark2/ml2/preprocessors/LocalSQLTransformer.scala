package io.hydrosphere.mist.lib.spark2.ml2.preprocessors

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.feature.SQLTransformer


object LocalSQLTransformer extends LocalModel[SQLTransformer] {
  override def load(metadata: Metadata, data: Map[String, Any]): SQLTransformer = {
    new SQLTransformer(metadata.uid).setStatement(metadata.paramMap("statement").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: SQLTransformer): LocalTransformer[SQLTransformer] = ???
}
