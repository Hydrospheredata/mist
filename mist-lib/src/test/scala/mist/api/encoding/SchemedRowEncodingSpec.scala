package mist.api.encoding

import mist.api.TestSparkContext
import mist.api.data._
import mist.api.encoding.spark.SchemedRowEncoder
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpec, Matchers}

case class TestData(
  intF: Int,
  stringF: String,
  doubleF: Double,
  longF: Long
)

class SchemedRowEncoderSpec extends FunSpec with Matchers with TestSparkContext {

  it("should encode") {
    val sqlCtx = new SQLContext(spark)

    val df = sqlCtx.createDataFrame(
      Seq(TestData(1, "string", 1.1, 1L))
    )

    val encoder = new SchemedRowEncoder(df.schema)
    val data = encoder.encode(df.queryExecution.toRdd.collect()(0))

    data shouldBe JsMap(
      "intF" -> JsNumber(1),
      "stringF" -> JsString("string"),
      "doubleF" -> JsNumber(1.1),
      "longF" -> JsNumber(1L)
    )
  }

}
