import mist.api.{Extracted, Failed, FnContext}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import mist.api.data._
import mist.api.encoding.defaults._
import mist.api.encoding.JsSyntax._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Success

class TestExampleSpec extends FunSpec with Matchers with BeforeAndAfterAll{

  it("should correctly extract arg") {
    val one = JsMap(
      "numbers" -> Seq(1,2,3,4,5).js,
      "mult" -> 10.js
    )
    val two = JsMap(
      "numbers" -> Seq(1,2,3,4,5).js
    )

    val oneExp = Extracted((Seq(1,2,3,4,5), 10))
    val twoExp = Extracted((Seq(1,2,3,4,5), 2))

    val in = TestingExample.in
    in.extract(FnContext(spark, one)) shouldBe oneExp
    in.extract(FnContext(spark, two)) shouldBe twoExp
    in.extract(FnContext(spark, JsMap.empty)) shouldBe a[Failed]
  }

  it("should multiply") {
    val in = JsMap(
      "numbers" -> Seq(1,2,3,4,5).js,
      "mult" -> 10.js
    )
    val ctx = FnContext(spark, in)

    val out = TestingExample.raw.invoke(ctx)
    out shouldBe a[Success[_]]

    val result = out.get
    result.toSeq should contain theSameElementsInOrderAs Seq(10, 20, 30, 40, 50)
  }

  it("should work fully") {
    val in = JsMap(
      "numbers" -> Seq(1,2,3,4,5).js,
      "mult" -> 10.js
    )
    val ctx = FnContext(spark, in)

    val out = TestingExample.execute(ctx)
    out shouldBe a[Success[_]]

    val result = out.get
    result shouldBe JsList(Seq(10.js, 20.js, 30.js, 40.js, 50.js))
  }

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
