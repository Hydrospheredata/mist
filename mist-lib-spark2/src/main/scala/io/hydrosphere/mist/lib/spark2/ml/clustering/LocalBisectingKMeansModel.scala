package io.hydrosphere.mist.lib.spark2.ml.clustering

import io.hydrosphere.mist.lib.spark2.ml._
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
object LocalBisectingKMeansModel extends LocalModel[BisectingKMeansModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): BisectingKMeansModel = {
    val metaFromData: Map[String, Any] = Map("rootId" -> -1) // TODO: change it to data from parquet file
    val rootId = metaFromData.get("rootId")

    val constructor = classOf[BisectingKMeansModel].getDeclaredConstructor(classOf[String], classOf[BisectingKMeansModel])
    constructor.setAccessible(true)

    var inst = constructor.newInstance(metadata.uid)
    inst = inst.set(inst.featuresCol, metadata.paramMap("featuresCol").asInstanceOf[String])
    inst = inst.set(inst.predictionCol, metadata.paramMap("predictionCol").asInstanceOf[String])
    inst
  }

  override implicit def getTransformer(transformer: BisectingKMeansModel): LocalTransformer[BisectingKMeansModel] = ???
}
