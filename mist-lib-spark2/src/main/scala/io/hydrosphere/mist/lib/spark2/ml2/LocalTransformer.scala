package io.hydrosphere.mist.lib.spark2.ml2

import org.apache.spark.ml.Transformer

/**
  * Created by bulat on 22.03.17.
  */
trait LocalTransformer[T <: Transformer] {
  val sparkTransformer: T
  def transform(localData: LocalData): LocalData
}