package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{DataUtils, LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}


object LocalMaxAbsScaler extends LocalTypedTransformer[MaxAbsScalerModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val maxAbsList = data("maxAbs").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val maxAbs = new DenseVector(maxAbsList)

    val constructor = classOf[MaxAbsScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, maxAbs)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transformTyped(maxAbsScaler: MaxAbsScalerModel, localData: LocalData): LocalData = {
    localData.column(maxAbsScaler.getInputCol) match {
      case Some(column) =>
        val maxAbsUnzero = Vectors.dense(maxAbsScaler.maxAbs.toArray.map(x => if (x == 0) 1 else x))
        val newData = column.data.map(r => {
          val vec: List[Double] = r match {
            case d: List[Any @unchecked] =>
              val l: List[Double] = d map (_.toString.toDouble)
              l
            case d => throw new IllegalArgumentException(s"Unknown data type for LocalMaxAbsScaler: $d")
          }
          val brz = DataUtils.asBreeze(vec.toArray) / DataUtils.asBreeze(maxAbsUnzero.toArray)
          DataUtils.fromBreeze(brz)
        })
        localData.withColumn(LocalDataColumn(maxAbsScaler.getOutputCol, newData))
      case None => localData
    }
  }
}
