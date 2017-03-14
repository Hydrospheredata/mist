package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.Transformer


object LocalVectorSlicer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    var slicer = new VectorSlicer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("indices").foreach{ x => slicer = slicer.setIndices(x.asInstanceOf[Array[Int]])}
    metadata.paramMap.get("names").foreach{ x => slicer = slicer.setNames(x.asInstanceOf[Array[String]])}

    slicer
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
