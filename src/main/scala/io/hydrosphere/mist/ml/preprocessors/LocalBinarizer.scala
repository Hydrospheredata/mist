package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Binarizer


object LocalBinarizer extends LocalTypedTransformer[Binarizer] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Binarizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setThreshold(metadata.paramMap("threshold").toString.toDouble)
  }

  override def transformTyped(binarizer: Binarizer, localData: LocalData): LocalData = {
    localData.column(binarizer.getInputCol) match {
      case Some(column) =>
        val trashhold: Double = binarizer.getThreshold
        val newData = column.data.map(r => {
          if (r.asInstanceOf[Double] > trashhold) 1.0 else 0.0
        })
        localData.withColumn(LocalDataColumn(binarizer.getOutputCol, newData))
      case None => localData
    }
  }
}
