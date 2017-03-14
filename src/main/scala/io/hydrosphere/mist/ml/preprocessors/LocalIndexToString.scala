package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.SparkException
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.IndexToString

object LocalIndexToString extends LocalTypedTransformer[IndexToString] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    println(data)
    println(metadata)
    val ctor = classOf[IndexToString].getDeclaredConstructor(classOf[String])
    ctor.setAccessible(true)
    ctor
      .newInstance(metadata.uid)
      .setLabels(metadata.paramMap("labels").asInstanceOf[List[String]].to[Array])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transformTyped(indxStr: IndexToString, localData: LocalData): LocalData = {
    localData.column(indxStr.getInputCol) match {
      case Some(column) =>
        val labels = indxStr.getLabels
        val indexer = (index: Double) => {
          val idx = index.toInt
          if (0 <= idx && idx < labels.length) {
            labels(idx)
          } else {
            throw new SparkException(s"Unseen index: $index ??")
          }
        }
        val newColumn = LocalDataColumn(indxStr.getOutputCol, column.data map {
          case d: Double => indexer(d)
          case d => throw new IllegalArgumentException(s"Unknown data to index: $d")
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
