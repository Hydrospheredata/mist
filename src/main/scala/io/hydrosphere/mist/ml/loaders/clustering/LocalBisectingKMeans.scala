package io.hydrosphere.mist.ml.loaders.clustering

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.clustering.BisectingKMeans


object LocalBisectingKMeans extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): BisectingKMeans = {
    val constructor = classOf[BisectingKMeans].getDeclaredConstructor(classOf[String])
    constructor.setAccessible(true)

    var bkmeans = constructor.newInstance(metadata.uid)

    bkmeans
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setK(metadata.paramMap("K").asInstanceOf[Int])

    metadata.paramMap.get("predictionCol").foreach{ x => bkmeans = bkmeans.setPredictionCol(x.asInstanceOf[String])}
    metadata.paramMap.get("seed").foreach{ x => bkmeans = bkmeans.setSeed(x.asInstanceOf[Long])}
    metadata.paramMap.get("maxIter").foreach{ x => bkmeans = bkmeans.setMaxIter(x.asInstanceOf[Int])}
    metadata.paramMap.get("minDivisibleClusterSize").foreach{ x => bkmeans = bkmeans.setMinDivisibleClusterSize(x.asInstanceOf[Double])}

    bkmeans
  }
}
