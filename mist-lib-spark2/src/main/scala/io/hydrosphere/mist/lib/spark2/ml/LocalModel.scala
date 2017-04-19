package io.hydrosphere.mist.lib.spark2.ml

import org.apache.spark.ml.Transformer

trait LocalModel[T <: Transformer] {
  def load(metadata: Metadata, data: Map[String, Any]): T
  implicit def getTransformer(transformer: T): LocalTransformer[T]
}