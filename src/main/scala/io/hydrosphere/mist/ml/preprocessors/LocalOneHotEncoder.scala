package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.OneHotEncoder

object LocalOneHotEncoder extends LocalModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    var ohe = new OneHotEncoder(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("dropLast").foreach{ x => ohe = ohe.setDropLast(x.asInstanceOf[Boolean])}

    ohe
  }

  override def transform(sparkTransformer: Transformer, localData: LocalData): LocalData = ???
}
