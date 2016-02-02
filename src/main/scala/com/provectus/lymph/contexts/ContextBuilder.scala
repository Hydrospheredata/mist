package com.provectus.lymph.contexts

import com.provectus.lymph.LymphConfig
import org.apache.spark.{SparkContext, SparkConf}

private[lymph] object ContextBuilder {

  def namedSparkContext(name: String) = {

    val sparkConf = new SparkConf()
      .setMaster(LymphConfig.Spark.master)
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    NamedContextWrapper(new SparkContext(sparkConf), name)
  }
}
