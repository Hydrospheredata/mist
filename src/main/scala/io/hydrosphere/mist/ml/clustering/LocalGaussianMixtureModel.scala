package io.hydrosphere.mist.ml.clustering

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.stat.distribution.MultivariateGaussian
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}


/**
  * Local model loader for GaussianMixture clusterer
  *
  * @author IceKhan
  */
object LocalGaussianMixtureModel extends LocalTypedTransformer[GaussianMixtureModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val weights = data("weights").asInstanceOf[List[Double]].toArray
    val mus = data("mus").asInstanceOf[List[Vector]].toArray
    val sigmas = data("sigmas").asInstanceOf[List[Matrix]].toArray
    val gaussians = mus zip sigmas map {
      case (mu, sigma) => new MultivariateGaussian(mu, sigma)
    }

    val constructor = classOf[GaussianMixtureModel].getDeclaredConstructor(
        classOf[String],
        classOf[Array[Double]],
        classOf[Array[MultivariateGaussian]]
    )
    constructor.setAccessible(true)
    var inst = constructor.newInstance(metadata.uid, weights, gaussians)
    inst = inst.set(inst.probabilityCol, metadata.paramMap("probabilityCol").asInstanceOf[String])
    inst = inst.set(inst.featuresCol, metadata.paramMap("featuresCol").asInstanceOf[String])
    inst = inst.set(inst.predictionCol, metadata.paramMap("predictionCol").asInstanceOf[String])
    inst
  }

  override def transformTyped(gaussianModel: GaussianMixtureModel, localData: LocalData): LocalData = {
    localData.column(gaussianModel.getFeaturesCol) match {
      case Some(column) =>
        val predictMethod = classOf[GaussianMixtureModel].getMethod("predict", classOf[Vector])
        predictMethod.setAccessible(true)
        val newColumn = LocalDataColumn(gaussianModel.getPredictionCol, column.data map { feature =>
          predictMethod.invoke(gaussianModel, feature.asInstanceOf[Vector]).asInstanceOf[Int]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
