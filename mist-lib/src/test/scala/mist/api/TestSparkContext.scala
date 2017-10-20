package mist.api

import org.apache.spark.{SparkConf, SparkContext}

object TestSparkContext {

  lazy val sc: SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("mist-lib-test")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.ui.disabled", "true")
    new SparkContext(conf)
  }
}
