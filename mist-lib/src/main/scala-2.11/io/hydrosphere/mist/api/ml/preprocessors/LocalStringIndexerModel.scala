package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.SparkException
import org.apache.spark.ml.feature.StringIndexerModel

import scala.collection.mutable

class LocalStringIndexerModel(override val sparkTransformer: StringIndexerModel) extends LocalTransformer[StringIndexerModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val labelToIndex = {
          val n = sparkTransformer.labels.length
          val map = new mutable.HashMap[String, Double]
          var i = 0
          while (i < n) {
            map.update(sparkTransformer.labels(i), i)
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
        val newColumn = LocalDataColumn(sparkTransformer.getOutputCol, column.data map { feature =>
          val str = feature.asInstanceOf[String]
          indexer(str)
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalStringIndexerModel extends LocalModel[StringIndexerModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): StringIndexerModel = {
    new StringIndexerModel(metadata.uid, data("labels").asInstanceOf[List[String]].to[Array])
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setHandleInvalid(metadata.paramMap("handleInvalid").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: StringIndexerModel): LocalTransformer[StringIndexerModel] = new LocalStringIndexerModel(transformer)
}
