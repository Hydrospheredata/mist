package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.linalg.{DenseVector, Vector, SparseVector}

class LocalMinMaxScalerModel(override val sparkTransformer: MinMaxScalerModel) extends LocalTransformer[MinMaxScalerModel] {
  override def transform(localData: LocalData): LocalData = {
    val originalRange = (DataUtils.asBreeze(sparkTransformer.originalMax.toArray) - DataUtils.asBreeze(sparkTransformer.originalMin.toArray)).toArray
    val minArray = sparkTransformer.originalMin.toArray
    val min = sparkTransformer.getMin
    val max = sparkTransformer.getMax

    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newData = column.data.map(r => {
          val scale = max - min
          val vec: List[Double] = r match {
            case d: List[Any] => d map (_.toString.toDouble)
            case d: DenseVector => d.toArray.toList
            case d: SparseVector => d.toDense.toArray.toList
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
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalMinMaxScalerModel extends LocalModel[MinMaxScalerModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): MinMaxScalerModel = {
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

  override implicit def getTransformer(transformer: MinMaxScalerModel): LocalTransformer[MinMaxScalerModel] = new LocalMinMaxScalerModel(transformer)
}
