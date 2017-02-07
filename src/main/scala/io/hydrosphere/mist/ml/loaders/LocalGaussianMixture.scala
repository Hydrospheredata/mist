package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.clustering.GaussianMixture


object LocalGaussianMixture extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): GaussianMixture = {
    val constructor = classOf[GaussianMixture].getDeclaredConstructor(classOf[String])
    constructor.setAccessible(true)

    val gmixture = constructor.newInstance(metadata.uid)

    gmixture
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setK(metadata.paramMap("K").asInstanceOf[Int])

    metadata.paramMap.get("predictionCol").map{x => gmixture.setPredictionCol(x.asInstanceOf[String])}
    metadata.paramMap.get("maxIter").map{x => gmixture.setMaxIter(x.asInstanceOf[Int])}
    metadata.paramMap.get("seed").map{x => gmixture.setSeed(x.asInstanceOf[Long])}
    metadata.paramMap.get("tol").map{x => gmixture.setTol(x.asInstanceOf[Double])}
    metadata.paramMap.get("probabilityCol").map{x => gmixture.setProbabilityCol(x.asInstanceOf[String])}

    gmixture
  }
}
