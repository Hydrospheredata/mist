package io.hydrosphere.mist.ml

import io.hydrosphere.mist.lib.LocalData
import org.apache.spark.ml.Transformer

trait LocalModel {
  def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer
  def transform(sparkTransformer: Transformer, localData: LocalData): LocalData
}

trait LocalTypedTransformer[T <: Transformer] extends LocalModel {
  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = {
    transformTyped(sparkTransformer.asInstanceOf[T], localData)
  }
  def transformTyped(transformer: T, localData: LocalData): LocalData
}
