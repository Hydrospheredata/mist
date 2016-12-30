package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer

object LocalTokenizer extends LocalModel {
  
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Tokenizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}
