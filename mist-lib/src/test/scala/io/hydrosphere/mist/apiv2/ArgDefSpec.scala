package io.hydrosphere.mist.apiv2

import org.scalatest.{Matchers, FunSpec}

class ArgDefSpec extends FunSpec with Matchers {

  describe("const/missing") {

    it("should extract const") {
      ArgDef.const("1").extract(testCtx()) shouldBe Extracted("1")
    }

    it("should extract missing") {
      ArgDef.missing("msg").extract(testCtx()) shouldBe Missing("msg")
    }
  }

  describe("combine") {

    it("should extract combined") {
      val combined = ArgDef.const("1") combine ArgDef.const("2")
      val data = combined.extract(testCtx())
      println(data)
    }

    it("should fail all") {
      val combined = ArgDef.const("1") combine ArgDef.missing[Int]("msg1") combine ArgDef.missing[Int]("msg2")
      val data = combined.extract(testCtx())
      println(data)
    }
  }

  def testCtx(params: Map[String, Any] = Map.empty): JobContext = {
    JobContext(null, params)
  }
}
