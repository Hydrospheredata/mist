package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StopWordsRemover


object LocalStopWordsRemover extends LocalModel[StopWordsRemover] {
  override def load(metadata: Metadata, data: Map[String, Any]): StopWordsRemover = {
    var remover = new StopWordsRemover(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("stopWords").foreach{ x => remover = remover.setStopWords(x.asInstanceOf[Array[String]])}
    metadata.paramMap.get("caseSensitive").foreach{ x => remover = remover.setCaseSensitive(x.asInstanceOf[Boolean])}

    remover
  }

  override implicit def getTransformer(transformer: StopWordsRemover): LocalTransformer[StopWordsRemover] = ???
}
