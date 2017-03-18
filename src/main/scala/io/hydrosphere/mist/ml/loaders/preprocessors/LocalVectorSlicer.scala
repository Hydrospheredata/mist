package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.Transformer


object LocalVectorSlicer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new VectorSlicer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setNames(metadata.paramMap("names").asInstanceOf[List[String]].toArray)
      .setIndices(metadata.paramMap("indices").asInstanceOf[List[Int]].toArray)
  }
}
