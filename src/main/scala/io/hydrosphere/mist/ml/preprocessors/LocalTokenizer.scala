package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.Tokenizer

object LocalTokenizer extends LocalTypedTransformer[Tokenizer] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Tokenizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override def transformTyped(tokenizer: Tokenizer, localData: LocalData): LocalData = {
    localData.column(tokenizer.getInputCol) match {
      case Some(column) =>
        val method = classOf[Tokenizer].getMethod("createTransformFunc")
        val newData = column.data.map(s => {
          method.invoke(tokenizer).asInstanceOf[String => Seq[String]](s.asInstanceOf[String])
        })
        localData.withColumn(LocalDataColumn(tokenizer.getOutputCol, newData))
      case None => localData
    }
  }
}
