package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.mllib.feature.{StandardScalerModel => OldStandardScalerModel}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.ml.linalg.{DenseVector, Vector}

object LocalStandardScaler extends LocalTypedTransformer[StandardScalerModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[StandardScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Vector])
    constructor.setAccessible(true)

    val stdVals = data("std").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
    val std = new DenseVector(stdVals)

    val meanVals = data("mean").asInstanceOf[Map[String, Any]].getOrElse("values", List()).asInstanceOf[List[Double]].toArray
    val mean = new DenseVector(meanVals)
    constructor
      .newInstance(metadata.uid, std, mean)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transformTyped(standardScaler: StandardScalerModel, localData: LocalData): LocalData = {
    localData.column(standardScaler.getInputCol) match {
      case Some(column) =>
        val scaler = new OldStandardScalerModel(
          OldVectors.fromML(standardScaler.std.asInstanceOf[Vector]),
          OldVectors.fromML(standardScaler.mean.asInstanceOf[Vector]),
          standardScaler.getWithStd,
          standardScaler.getWithMean
        )

        val newData = column.data.map(r => {
          val vec: List[Double] = r match {
            case d: List[Any @unchecked] =>
              val l: List[Double] = d map (_.toString.toDouble)
              l
            case d => throw new IllegalArgumentException(s"Unknown data type for LocalStandardScaler: $d")
          }
          val vector: OldVector = OldVectors.dense(vec.toArray)
          scaler.transform(vector)
        })
        localData.withColumn(LocalDataColumn(standardScaler.getOutputCol, newData))
      case None => localData
    }
  }
}
