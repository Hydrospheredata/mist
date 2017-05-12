package io.hydrosphere.mist.api.ml.clustering

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

class LocalGaussianMixtureModel(override val sparkTransformer: GaussianMixtureModel) extends LocalTransformer[GaussianMixtureModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val predictMethod = classOf[GaussianMixtureModel].getMethod("predict", classOf[Vector])
        predictMethod.setAccessible(true)
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data map { feature =>
          predictMethod.invoke(sparkTransformer, feature.asInstanceOf[Vector]).asInstanceOf[Int]
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalGaussianMixtureModel extends LocalModel[GaussianMixtureModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): GaussianMixtureModel = {
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


  override implicit def getTransformer(transformer: GaussianMixtureModel): LocalTransformer[GaussianMixtureModel] = new LocalGaussianMixtureModel(transformer)
}
