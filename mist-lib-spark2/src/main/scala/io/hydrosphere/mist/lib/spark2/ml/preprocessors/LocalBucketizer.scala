package io.hydrosphere.mist.lib.spark2.ml.preprocessors

import io.hydrosphere.mist.lib.spark2.ml._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.Bucketizer


object LocalBucketizer extends LocalModel[Bucketizer] {
  override def load(metadata: Metadata, data: Map[String, Any]): Bucketizer = {
    var bucketizer = new Bucketizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setSplits(metadata.paramMap("splits").asInstanceOf[Array[Double]])

    metadata.paramMap.get("parent").foreach{ x => bucketizer = bucketizer.setParent(x.asInstanceOf[Estimator[Bucketizer]])}

    bucketizer
  }

  override implicit def getTransformer(transformer: Bucketizer): LocalTransformer[Bucketizer] = ???
}
