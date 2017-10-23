package mist.api.encoding

import mist.api.TestSparkContext
import mist.api.data._
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpec, Matchers}

case class TestData(
  intF: Int,
  stringF: String,
  doubleF: Double,
  longF: Long
)

class SchemedRowEncoderSpec extends FunSpec with Matchers {

  it("should encode") {
    val sc = TestSparkContext.sc
    val sqlCtx = new SQLContext(sc)

    val df = sqlCtx.createDataFrame(
      Seq(TestData(1, "string", 1.1, 1L))
    )

    val encoder = new SchemedRowEncoder(df.schema)
    val data = encoder.encode(df.queryExecution.toRdd.collect()(0))

    data shouldBe JsLikeMap(
      "intF" -> JsLikeNumber(1),
      "stringF" -> JsLikeString("string"),
      "doubleF" -> JsLikeNumber(1.1),
      "longF" -> JsLikeNumber(1L)
    )
  }

}
