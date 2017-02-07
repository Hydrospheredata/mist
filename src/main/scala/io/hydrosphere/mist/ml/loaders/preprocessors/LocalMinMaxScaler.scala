package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.MinMaxScaler


object LocalMinMaxScaler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MinMaxScaler = {
    var scaler = new MinMaxScaler(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("min").foreach{ x => scaler = scaler.setMin(x.asInstanceOf[Double])}
    metadata.paramMap.get("max").foreach{ x => scaler = scaler.setMax(x.asInstanceOf[Double])}

    scaler
  }
}
