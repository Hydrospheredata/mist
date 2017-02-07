package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.ChiSqSelector


object LocalChiSqSelector extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): ChiSqSelector = {
    var selector = new ChiSqSelector(metadata.uid)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setLabelCol(metadata.paramMap("labelCol").asInstanceOf[String])

    metadata.paramMap.get("topFeatures").foreach{ x => selector = selector.setNumTopFeatures(x.asInstanceOf[Int])}
    metadata.paramMap.get("fpr").foreach{ x => selector = selector.setFpr(x.asInstanceOf[Double])}
    metadata.paramMap.get("percentile").foreach{ x => selector = selector.setPercentile(x.asInstanceOf[Double])}
    metadata.paramMap.get("selectorType").foreach{ x => selector = selector.setSelectorType(x.asInstanceOf[String])}

    selector
  }
}
