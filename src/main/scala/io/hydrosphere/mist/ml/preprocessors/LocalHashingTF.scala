package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.mllib.feature.{HashingTF => HTF}

import scala.collection.mutable

object LocalHashingTF extends LocalTypedTransformer[HashingTF] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new HashingTF(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setBinary(metadata.paramMap("binary").asInstanceOf[Boolean])
      .setNumFeatures(metadata.paramMap("numFeatures").asInstanceOf[Int])
  }

  override def transformTyped(hashingTF: HashingTF, localData: LocalData): LocalData = {
    localData.column(hashingTF.getInputCol) match {
      case Some(column) =>
        val htf = new HTF(hashingTF.getNumFeatures).setBinary(hashingTF.getBinary)
        val newData = column.data.map((m) => htf.transform(m.asInstanceOf[mutable.WrappedArray[String]]))
        localData.withColumn(LocalDataColumn(hashingTF.getOutputCol, newData))
      case None => localData
    }
  }
}
