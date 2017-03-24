package io.hydrosphere.mist.lib.spark2.ml.loaders.clustering

import io.hydrosphere.mist.lib.spark2.ml.Metadata
import io.hydrosphere.mist.lib.spark2.ml.loaders.LocalModel
import org.apache.spark.ml.clustering.LDAModel
import org.apache.spark.ml.Transformer

object LocalLDA extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = ???
//  {
//    val constructor = classOf[LDA].getDeclaredConstructor(classOf[String])
//    constructor.setAccessible(true)
//    var lda = constructor.newInstance(metadata.uid)
//
//    lda
//      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
//      .setK(metadata.paramMap("K").asInstanceOf[Int])
//      .setMaxIter(metadata.paramMap("maxIter").asInstanceOf[Int])
//
//    metadata.paramMap.get("topicConcentration").foreach{ x => lda = lda.setTopicConcentration(x.asInstanceOf[Double])}
//    metadata.paramMap.get("topicDistributionCol").foreach{ x => lda = lda.setTopicDistributionCol(x.asInstanceOf[String])}
//    metadata.paramMap.get("checkpointInterval").foreach{ x => lda = lda.setCheckpointInterval(x.asInstanceOf[Int])}
//    metadata.paramMap.get("optimizer").foreach{ x => lda = lda.setOptimizer(x.asInstanceOf[String])}
//    metadata.paramMap.get("keepLastCheckpoint").foreach{ x => lda = lda.setKeepLastCheckpoint(x.asInstanceOf[Boolean])}
//    metadata.paramMap.get("learningDecay").foreach{ x => lda = lda.setLearningDecay(x.asInstanceOf[Double])}
//    metadata.paramMap.get("learningOffset").foreach{ x => lda = lda.setLearningOffset(x.asInstanceOf[Double])}
//    metadata.paramMap.get("docConcentration").foreach{ x => lda = lda.setDocConcentration(x.asInstanceOf[Array[Double]])}
//    metadata.paramMap.get("optimizeDocConcentration").foreach{ x => lda = lda.setOptimizeDocConcentration(x.asInstanceOf[Boolean])}
//    metadata.paramMap.get("seed").foreach{ x => lda = lda.setSeed(x.asInstanceOf[Long])}
//    metadata.paramMap.get("subsamplingRate").foreach{ x => lda = lda.setSubsamplingRate(x.asInstanceOf[Double])}
//
//    lda
//  }
}
