package io.hydrosphere.mist.ml.loaders.clustering

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.{KMeansModel => MLlibKMeans}
import org.apache.spark.mllib.linalg.{Vector => MLlibVec}

//TODO
object LocalKMeans extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): KMeansModel = {
    val parentConstructor = classOf[MLlibKMeans].getDeclaredConstructor(classOf[Array[MLlibVec]])
    parentConstructor.setAccessible(true)
    val mlk = parentConstructor.newInstance(metadata.paramMap("clustersCenter"))

    val constructor = classOf[KMeansModel].getDeclaredConstructor(classOf[String], classOf[MLlibVec])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, mlk)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
