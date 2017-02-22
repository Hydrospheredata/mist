package io.hydrosphere.mist.ml.loaders.preprocessors

import io.hydrosphere.mist.ml.Metadata
import io.hydrosphere.mist.ml.loaders.LocalModel
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{Vector, DenseVector}


object LocalMinMaxScaler extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    println("METADATA")
    println(metadata)
    println("DATA")
    println(data)

    val originalMinList = data("originalMin").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val originalMin = new DenseVector(originalMinList)

    val originalMaxList = data("originalMax").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val originalMax = new DenseVector(originalMaxList)

    val constructor = classOf[MinMaxScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Vector])
    constructor.setAccessible(true)
    constructor.newInstance(metadata.uid, originalMin, originalMax)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setMin(metadata.paramMap("min").toString.toDouble)
      .setMax(metadata.paramMap("max").toString.toDouble)
  }
}
