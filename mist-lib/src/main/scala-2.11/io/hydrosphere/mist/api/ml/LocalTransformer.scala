package io.hydrosphere.mist.api.ml

import org.apache.spark.ml.Transformer

/**
  * Created by bulat on 22.03.17.
  */
trait LocalTransformer[T <: Transformer] {
  val sparkTransformer: T
  def transform(localData: LocalData): LocalData
}