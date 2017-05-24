package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.StopWordsRemover

class LocalStopWordsRemover(override val sparkTransformer: StopWordsRemover) extends LocalTransformer[StopWordsRemover] {
  override def transform(localData: LocalData): LocalData = {
    val stopWordsSet = sparkTransformer.getStopWords
    val toLower = (s: String) => if (s != null) s.toLowerCase else s
    val lowerStopWords = stopWordsSet.map(toLower(_)).toSet

    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newData = column.data.map(r => {
          if (sparkTransformer.getCaseSensitive) {
            r.asInstanceOf[List[String]].filter(s => !stopWordsSet.contains(s))
          } else {
            r.asInstanceOf[List[String]].filter(s => !lowerStopWords.contains(toLower(s)))
          }
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalStopWordsRemover extends LocalModel[StopWordsRemover] {
  override def load(metadata: Metadata, data: Map[String, Any]): StopWordsRemover = {
    new StopWordsRemover(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setCaseSensitive(metadata.paramMap("caseSensitive").asInstanceOf[Boolean])
      .setStopWords(metadata.paramMap("stopWords").asInstanceOf[List[String]].toArray)
  }

  override implicit def getTransformer(transformer: StopWordsRemover): LocalTransformer[StopWordsRemover] = new LocalStopWordsRemover(transformer)
}
