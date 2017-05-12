package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.feature.{HashingTF => HTF}

import scala.collection.mutable

class LocalHashingTF(override val sparkTransformer: HashingTF) extends LocalTransformer[HashingTF] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val htf = new HTF(sparkTransformer.getNumFeatures).setBinary(sparkTransformer.getBinary)
        val newData = column.data.map((m) => htf.transform(m.asInstanceOf[mutable.WrappedArray[String]]))
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalHashingTF extends LocalModel[HashingTF] {
  override def load(metadata: Metadata, data: Map[String, Any]): HashingTF = {
    new HashingTF(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setBinary(metadata.paramMap("binary").asInstanceOf[Boolean])
      .setNumFeatures(metadata.paramMap("numFeatures").asInstanceOf[Number].intValue())
  }

  override implicit def getTransformer(transformer: HashingTF): LocalTransformer[HashingTF] = new LocalHashingTF(transformer)
}
