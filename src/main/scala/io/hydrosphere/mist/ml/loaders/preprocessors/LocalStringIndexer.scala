package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.Transformer

object LocalStringIndexer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new StringIndexerModel(metadata.uid, data("labels").asInstanceOf[List[String]].to[Array])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("indexedLabel").asInstanceOf[String])
      .setHandleInvalid(metadata.paramMap("handleInvalid").asInstanceOf[String])
  }
}
