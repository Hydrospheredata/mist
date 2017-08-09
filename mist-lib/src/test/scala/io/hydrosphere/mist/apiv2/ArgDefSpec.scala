package io.hydrosphere.mist.apiv2

import org.scalatest.{Matchers, FunSpec}
import shapeless.HNil

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
      val combined = ArgDef.const("first") &
        ArgDef.const("second") & ArgDef.const("third") & ArgDef.const(4)

      val data = combined.extract(testCtx())
      data shouldBe Extracted("first" :: "second" :: "third" :: 4 :: HNil)
    }

    it("should fail all") {
      val combined = ArgDef.const("1") combine ArgDef.missing[Int]("msg1") combine ArgDef.missing[Int]("msg2")
      val data = combined.extract(testCtx())
      data shouldBe Missing("msg1, msg2")
    }
  }

  def testCtx(params: Map[String, Any] = Map.empty): JobContext = {
    JobContext(null, params)
  }
}
