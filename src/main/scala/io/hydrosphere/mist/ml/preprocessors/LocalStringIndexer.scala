package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.Transformer

import scala.collection.mutable

object LocalStringIndexer extends LocalTypedTransformer[StringIndexerModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new StringIndexerModel(metadata.uid, data("labels").asInstanceOf[List[String]].to[Array])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setHandleInvalid(metadata.paramMap("handleInvalid").asInstanceOf[String])
  }

  override def transformTyped(strIndexer: StringIndexerModel, localData: LocalData): LocalData = {
    localData.column(strIndexer.getInputCol) match {
      case Some(column) =>
        val labelToIndex = {
          val n = strIndexer.labels.length
          val map = new mutable.HashMap[String, Double]
          var i = 0
          while (i < n) {
            map.update(strIndexer.labels(i), i)
            i += 1
          }
          map
        }
        val indexer = (label: String) => {
          if (labelToIndex.contains(label)) {
            labelToIndex(label)
          } else {
            throw new SparkException(s"Unseen label: $label.")
          }
        }
        val newColumn = LocalDataColumn(strIndexer.getOutputCol, column.data map { feature =>
          val str = feature.asInstanceOf[String]
          indexer(str)
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}
