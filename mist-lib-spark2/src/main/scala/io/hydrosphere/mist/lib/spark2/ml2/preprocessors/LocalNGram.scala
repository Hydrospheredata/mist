package io.hydrosphere.mist.lib.spark2.ml2.preprocessors

import io.hydrosphere.mist.lib.spark2.ml2._
import org.apache.spark.ml.feature.NGram

class LocalNGram(override val sparkTransformer: NGram) extends LocalTransformer[NGram] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val method = classOf[NGram].getMethod("createTransformFunc")
        val newData = column.data.map(r => {
          method.invoke(sparkTransformer).asInstanceOf[Seq[String] => Seq[String]](r.asInstanceOf[Seq[String]])
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalNGram extends LocalModel[NGram] {
  override def load(metadata: Metadata, data: Map[String, Any]): NGram = {
    new NGram(metadata.uid)
        .setN(metadata.paramMap("n").asInstanceOf[Int])
        .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
        .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: NGram): LocalTransformer[NGram] = new LocalNGram(transformer)
}
