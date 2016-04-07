package com.provectus.mist.contexts

import com.provectus.mist.MistConfig
import org.apache.spark.{SparkContext, SparkConf}

/** Builds spark contexts with necessary settings */
private[mist] object ContextBuilder {

  /** Build contexts with namespace
    *
    * @param name namespace
    * @return [[ContextWrapper]] with prepared context
    */
  def namedSparkContext(name: String): ContextWrapper = {

    val sparkConf = new SparkConf()
      .setMaster(MistConfig.Spark.master)
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    NamedContextWrapper(new SparkContext(sparkConf), name)
  }
}
