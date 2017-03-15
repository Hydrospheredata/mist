package io.hydrosphere.mist.lib.spark2.ml.loaders.preprocessors

import io.hydrosphere.mist.lib.spark2.ml.{Metadata, DataUtils}
import io.hydrosphere.mist.ml.DataUtils
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.VectorIndexerModel


object LocalVectorIndexer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val ctor = classOf[VectorIndexerModel].getDeclaredConstructor(
      classOf[String],
      classOf[Int],
      classOf[Map[Int, Map[Double, Int]]]
    )
    ctor.setAccessible(true)
    ctor
      .newInstance(
        metadata.uid,
        data("numFeatures").asInstanceOf[java.lang.Integer],
        DataUtils.flatConvertMap(data("categoryMaps").asInstanceOf[Map[String, Any]])
      )
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

}
