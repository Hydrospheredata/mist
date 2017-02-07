package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.QuantileDiscretizer


object LocalQuantileDiscretizer extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): QuantileDiscretizer = {
    var qd = new QuantileDiscretizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setNumBuckets(metadata.paramMap("numBuckets").asInstanceOf[Int])

    metadata.paramMap.get("handleInvalid").foreach{ x => qd = qd.setHandleInvalid(x.asInstanceOf[String])}
    metadata.paramMap.get("relativeError").foreach{ x => qd = qd.setRelativeError(x.asInstanceOf[Double])}

    qd
  }
}
