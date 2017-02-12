package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Vectors}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix}


object LocalPCA extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[PCAModel].getDeclaredConstructor(classOf[String], classOf[DenseMatrix], classOf[DenseVector])
    constructor.setAccessible(true)
    if (data.contains("explainedVariance")) {
      // NOTE: Spark >= 2
      val pc = data("pc").asInstanceOf[DenseMatrix]
      val explainedVariance = data("explainedVariance").asInstanceOf[DenseVector]
      constructor
        .newInstance(metadata.uid, pc, explainedVariance)
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    } else {
      // NOTE: Spark < 2
      val pc = data("pc").asInstanceOf[OldDenseMatrix]
      constructor
        .newInstance(metadata.uid, pc.asML, Vectors.dense(Array.empty[Double]).asInstanceOf[DenseVector])
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
    }
  }
}
