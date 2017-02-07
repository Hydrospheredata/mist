package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.RFormula


object LocalRFormula extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): RFormula = {
    new RFormula(metadata.uid)
      .setFormula(metadata.paramMap("formula").asInstanceOf[String])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setForceIndexLabel(metadata.paramMap("forceIndexLabel").asInstanceOf[Boolean])
      .setLabelCol(metadata.paramMap("labelCol").asInstanceOf[String])
  }
}
