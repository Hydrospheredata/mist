package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.NGram

class LocalNGram(override val sparkTransformer: NGram) extends LocalTransformer[NGram] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[NGram].getMethod("createTransformFunc")
        val f = method.invoke(sparkTransformer).asInstanceOf[Seq[String] => Seq[String]]
        val data = column.data.head.asInstanceOf[Seq[String]]
        val newData = f.apply(data).toList
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, List(newData)))
      case None => localData
    }
  }
}

object LocalNGram extends LocalModel[NGram] {
  override def load(metadata: Metadata, data: Map[String, Any]): NGram = {
    new NGram(metadata.uid)
        .setN(metadata.paramMap("n").asInstanceOf[Number].intValue())
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: NGram): LocalTransformer[NGram] = new LocalNGram(transformer)
}
