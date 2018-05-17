package io.hydrosphere.mist.worker

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSpec}

class SparkUtilsSpec extends FunSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  it("should extract spark ui") {
    val result = SparkUtils.getSparkUiAddress(sc)
    result.isDefined shouldBe true

    val address = result.get
    address should endWith(":5051")

    sc.stop()
  }

  override def beforeAll = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("test")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.ui.port", "5051")

    sc = SparkContext.getOrCreate(conf)
    //sc = new SparkContext(conf)
  }

  override def afterAll {
    //sc.stop()
  }

}
