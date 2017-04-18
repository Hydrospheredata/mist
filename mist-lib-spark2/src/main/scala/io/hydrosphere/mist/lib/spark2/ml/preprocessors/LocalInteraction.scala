package io.hydrosphere.mist.lib.spark2.ml.preprocessors

import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.feature.Interaction

object LocalInteraction extends LocalModel[Interaction] {
  override def load(metadata: Metadata, data: Map[String, Any]): Interaction = {
    new Interaction(metadata.uid)
      .setInputCols(metadata.paramMap("inputCols").asInstanceOf[Array[String]])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: Interaction): LocalTransformer[Interaction] = ???
}
