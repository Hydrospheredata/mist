package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StopWordsRemover


object LocalStopWordsRemover extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    var remover = new StopWordsRemover(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("stopWords").foreach{ x => remover = remover.setStopWords(x.asInstanceOf[Array[String]])}
    metadata.paramMap.get("caseSensitive").foreach{ x => remover = remover.setCaseSensitive(x.asInstanceOf[Boolean])}

    remover
  }
}
