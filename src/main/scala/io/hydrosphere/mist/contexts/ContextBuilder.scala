package io.hydrosphere.mist.contexts

import io.hydrosphere.mist.MistConfig
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
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkConfSettings = MistConfig.Contexts.sparkConf(name)

    for (keyValue: List[String] <- sparkConfSettings) {
      sparkConf.set(keyValue.head, keyValue(1))
    }

    NamedContextWrapper(new SparkContext(sparkConf), name)
  }
}
