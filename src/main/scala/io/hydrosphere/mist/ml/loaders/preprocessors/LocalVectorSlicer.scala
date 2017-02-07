package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.VectorSlicer


object LocalVectorSlicer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): VectorSlicer = {
    var slicer = new VectorSlicer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("indices").foreach{ x => slicer = slicer.setIndices(x.asInstanceOf[Array[Int]])}
    metadata.paramMap.get("names").foreach{ x => slicer = slicer.setNames(x.asInstanceOf[Array[String]])}

    slicer
  }
}
