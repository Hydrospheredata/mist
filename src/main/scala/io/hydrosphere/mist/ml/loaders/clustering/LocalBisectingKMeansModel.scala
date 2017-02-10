package io.hydrosphere.mist.ml.loaders.clustering

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.BisectingKMeansModel


/**
  * Local model loader for BisectingKMeans clusterer
  *
  * @author IceKhan
  *
  * TODO: implement reading of "/data/" folder during model loading
  *   metadata from "/data/" folder
  *   {"class":"org.apache.spark.mllib.clustering.BisectingKMeansModel","version":"1.0","rootId":-1}
  */
object LocalBisectingKMeansModel extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val metaFromData: Map[String, Any] = Map("rootId" -> -1) // TODO: change it to data from parquet file
    val rootId = metaFromData.get("rootId")

    val constructor = classOf[BisectingKMeansModel].getDeclaredConstructor(classOf[String], classOf[BisectingKMeansModel])
    constructor.setAccessible(true)

    constructor
      .newInstance(metadata.uid)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
  }
}
