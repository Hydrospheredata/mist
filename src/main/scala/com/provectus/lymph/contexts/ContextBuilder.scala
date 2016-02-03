package com.provectus.lymph.contexts

import com.provectus.lymph.LymphConfig
import org.apache.spark.{SparkContext, SparkConf}

/** Builds spark contexts with necessary settings */
private[lymph] object ContextBuilder {

  /** Build contexts with namespace
    *
    * @param name namespace
    * @return [[ContextWrapper]] with prepared context
    */
  def namedSparkContext(name: String): ContextWrapper = {

    val sparkConf = new SparkConf()
      .setMaster(LymphConfig.Spark.master)
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    NamedContextWrapper(new SparkContext(sparkConf), name)
  }
}
