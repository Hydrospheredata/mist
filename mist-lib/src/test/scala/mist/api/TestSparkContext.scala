package mist.api

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait TestSparkContext extends BeforeAndAfterAll{this: Suite =>

  var spark: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("mist-lib-test" + this.getClass.getSimpleName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.ui.disabled", "true")
    spark = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
