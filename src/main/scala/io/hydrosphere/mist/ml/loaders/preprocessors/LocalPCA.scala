package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.PCA

object LocalPCA extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): PCA = {
    new PCA(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setK(metadata.paramMap("K").asInstanceOf[Int])
  }
}
