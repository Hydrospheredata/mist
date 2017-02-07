package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.MinHashLSH


object LocalMinHashLSH extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MinHashLSH = {
    var lsh = new MinHashLSH(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setNumHashTables(metadata.paramMap("numHashTables").asInstanceOf[Int])

    metadata.paramMap.get("seed").foreach{ x => lsh = lsh.setSeed(x.asInstanceOf[Long])}

    lsh
  }
}
