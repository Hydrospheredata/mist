package io.hydrosphere.mist.ml

import io.hydrosphere.mist.lib.LocalData
import org.apache.spark.ml.Transformer

/**
  * Created by bulat on 22.03.17.
  */
trait LocalTransformer[T <: Transformer] {
  val sparkTransformer: T
  def transform(localData: LocalData): LocalData
}