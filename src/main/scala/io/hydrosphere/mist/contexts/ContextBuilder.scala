package io.hydrosphere.mist.contexts

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.api.ContextWrapper
import org.apache.spark.{SparkConf, SparkContext}

/** Builds spark contexts with necessary settings */
private[mist] object ContextBuilder {

  /** Build contexts with namespace
    *
    * @param namespace namespace
    * @return [[io.hydrosphere.mist.lib.spark1.ContextWrapper]] with prepared context
    */
  def namedSparkContext(namespace: String): ContextWrapper = {

    val sparkConf = new SparkConf()
      .setAppName(namespace)
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkConfSettings = MistConfig.Contexts.sparkConf(namespace)

    for (keyValue: List[String] <- sparkConfSettings) {
      sparkConf.set(keyValue.head, keyValue(1))
    }

    NamedContextWrapper(new SparkContext(sparkConf), namespace)
  }
}
