package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.OneHotEncoder

object LocalOneHotEncoder extends LocalModel[OneHotEncoder] {
  override def load(metadata: Metadata, data: Map[String, Any]): OneHotEncoder = {
    var ohe = new OneHotEncoder(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])

    metadata.paramMap.get("dropLast").foreach{ x => ohe = ohe.setDropLast(x.asInstanceOf[Boolean])}

    ohe
  }

  override implicit def getTransformer(transformer: OneHotEncoder): LocalTransformer[OneHotEncoder] = ???
}
