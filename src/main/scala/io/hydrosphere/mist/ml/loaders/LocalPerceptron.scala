package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}

object LocalPerceptron extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MultilayerPerceptronClassificationModel = {
    val constructor = classOf[MultilayerPerceptronClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])
    constructor.setAccessible(true)
    constructor.newInstance(metadata.uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[Map[String, Any]]("values").asInstanceOf[List[Double]].toArray))
  }
}
