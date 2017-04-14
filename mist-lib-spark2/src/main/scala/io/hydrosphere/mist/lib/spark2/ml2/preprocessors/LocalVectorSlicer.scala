package io.hydrosphere.mist.lib.spark2.ml2.preprocessors

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.feature.VectorSlicer


object LocalVectorSlicer extends LocalModel[VectorSlicer] {
  override def load(metadata: Metadata, data: Map[String, Any]): VectorSlicer = {
    new VectorSlicer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setNames(metadata.paramMap("names").asInstanceOf[List[String]].toArray)
      .setIndices(metadata.paramMap("indices").asInstanceOf[List[Int]].toArray)
  }

  override implicit def getTransformer(transformer: VectorSlicer): LocalTransformer[VectorSlicer] = ???
}
