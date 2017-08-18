package io.hydrosphere.mist.worker

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FunSpec}

class SparkUtilsSpec extends FunSpec with Matchers {

  it("should extract spark ui") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.ui.port", "5051")

    val sc = new SparkContext(conf)

    val result = SparkUtils.getSparkUiAddress(sc)
    result.isDefined shouldBe true

    val address = result.get
    address should endWith(":5051")
  }
}
