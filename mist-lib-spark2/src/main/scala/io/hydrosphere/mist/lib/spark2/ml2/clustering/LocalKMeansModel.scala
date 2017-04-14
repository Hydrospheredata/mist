package io.hydrosphere.mist.lib.spark2.ml2.clustering

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.{KMeansModel => MLlibKMeans}
import org.apache.spark.mllib.linalg.{Vector => MLlibVec}

//TODO
object LocalKMeansModel extends LocalModel[KMeansModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): KMeansModel = ???
//  {
//    val parentConstructor = classOf[MLlibKMeans].getDeclaredConstructor(classOf[Array[MLlibVec]])
//    parentConstructor.setAccessible(true)
//    val mlk = parentConstructor.newInstance(metadata.paramMap("clustersCenter"))
//
//    val constructor = classOf[KMeansModel].getDeclaredConstructor(classOf[String], classOf[MLlibVec])
//    constructor.setAccessible(true)
//    constructor
//      .newInstance(metadata.uid, mlk)
//      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
//      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
//  }
  override implicit def getTransformer(transformer: KMeansModel): LocalTransformer[KMeansModel] = ???
}
