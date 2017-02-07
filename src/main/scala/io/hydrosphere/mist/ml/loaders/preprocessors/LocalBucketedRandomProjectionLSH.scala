package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH


object LocalBucketedRandomProjectionLSH extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): BucketedRandomProjectionLSH = {
    var res = new BucketedRandomProjectionLSH(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setBucketLength(metadata.paramMap("bucketLength").asInstanceOf[Double])
      .setNumHashTables(metadata.paramMap("numHashTables").asInstanceOf[Int])

    metadata.paramMap.get("seed").foreach{ x => res = res.setSeed(x.asInstanceOf[Long])}

    res
  }
}
