package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.LocalData
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import io.hydrosphere.mist.utils.SparkUtils
import org.apache.spark.ml.{Estimator, Transformer}
import org.apache.spark.ml.feature.Bucketizer


object LocalBucketizer extends LocalTypedTransformer[Bucketizer] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    var bucketizer = new Bucketizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setSplits(metadata.paramMap("splits").asInstanceOf[Array[Double]])

    metadata.paramMap.get("parent").foreach{ x => bucketizer = bucketizer.setParent(x.asInstanceOf[Estimator[Bucketizer]])}

    bucketizer
  }

  override def transformTyped(transformer: Bucketizer, localData: LocalData): LocalData = ???
}
