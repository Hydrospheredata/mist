package mist.api.args

import org.scalatest.{FunSpec, Matchers}

class DepFnSpec extends FunSpec with Matchers {

  describe("JoinToTuple") {

    it("should join") {
      JoinToTuple((1, "sad"), 3) shouldBe (1, "sad", 3)
      JoinToTuple(1, "x") shouldBe (1, "x")
      JoinToTuple(("z", 12), ("d", false, "asd")) shouldBe ("z", 12, "d", false, "asd")
    }
  }

  describe("ArgCombiner") {
    it("should combine into tuples") {
      val arg1 = ArgDef.const("1")
      val arg2 = ArgDef.const(2)
      val arg3 = ArgDef.const(false)
      val combined: ArgDef[(String, Int, Boolean)] = arg1 & arg2 & arg3
      val next: ArgDef[(String, Int, Boolean, Long)] = combined & ArgDef.const(1L)
      val toTupled: ArgDef[(String, Int, Boolean, Long, String, Int, Boolean)] = next & combined
    }

  }
}

