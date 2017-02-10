package io.hydrosphere.mist.ml.loaders.clustering

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.linalg.{Vector, Matrix}


/**
  * Local model loader for GaussianMixture clusterer
  *
  * @author IceKhan
  */
object LocalGaussianMixtureModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val weights = data("weights").asInstanceOf[Array[Double]]
    val mus = data("mus").asInstanceOf[Array[Vector]]
    val sigmas = data("sigmas").asInstanceOf[Array[Matrix]]
    val gaussians = mus.zip(sigmas).map {
      case (mu, sigma) => new MultivariateGaussian(mu, sigma)
    }

    val constructor = classOf[GaussianMixtureModel]
      .getDeclaredConstructor(
        classOf[String],
        classOf[Array[Double]],
        classOf[Array[MultivariateGaussian]]
      )
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, weights, gaussians)
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
