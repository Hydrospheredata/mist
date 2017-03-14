package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import io.hydrosphere.mist.utils.DataUtils
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseVector, Vector}


object LocalMinMaxScaler extends LocalTypedTransformer[MinMaxScalerModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
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

  override def transformTyped(minMaxScaler: MinMaxScalerModel, localData: LocalData): LocalData = {
    val originalRange = (DataUtils.asBreeze(minMaxScaler.originalMax.toArray) - DataUtils.asBreeze(minMaxScaler.originalMin.toArray)).toArray
    val minArray = minMaxScaler.originalMin.toArray
    val min = minMaxScaler.getMin
    val max = minMaxScaler.getMax

    localData.column(minMaxScaler.getInputCol) match {
      case Some(column) =>
        val newData = column.data.map(r => {
          val scale = max - min
          val vec: List[Double] = r match {
            case d: List[Any] =>
              val l: List[Double] = d map (_.toString.toDouble)
              l
            case d => throw new IllegalArgumentException(s"Unknown data type for LocalMinMaxScaler: $d")
          }
          val values = vec.toArray
          val size = values.length
          var i = 0
          while (i < size) {
            if (!values(i).isNaN) {
              val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
              values(i) = raw * scale + min
            }
            i += 1
          }
          values
        })
        localData.withColumn(LocalDataColumn(minMaxScaler.getOutputCol, newData))
      case None => localData
    }
  }
}
