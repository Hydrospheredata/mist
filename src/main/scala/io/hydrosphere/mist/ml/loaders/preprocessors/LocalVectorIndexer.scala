package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorIndexer


object LocalVectorIndexer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): VectorIndexer = {
    new VectorIndexer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setMaxCategories(metadata.paramMap("maxCategories").asInstanceOf[Int])
  }
}
