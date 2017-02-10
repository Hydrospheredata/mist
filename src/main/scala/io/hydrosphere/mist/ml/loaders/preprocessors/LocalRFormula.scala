package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.Transformer


object LocalRFormula extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = ???
}
