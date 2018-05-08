package mist.api.internal

import mist.api.ArgDef
import org.scalatest.{FunSpec, Matchers}

class DepFnSpec extends FunSpec with Matchers {

  import mist.api.ArgsInstances._

  describe("JoinToTuple") {

    it("should join") {
      JoinToTuple((1, "sad"), 3) shouldBe (1, "sad", 3)
      JoinToTuple(1, "x") shouldBe (1, "x")
      JoinToTuple(("z", 12), ("d", false, "asd")) shouldBe ("z", 12, "d", false, "asd")
    }
  }

  describe("ArgCombiner") {
    it("should combine into tuples") {
      val arg1 = const("1")
      val arg2 = const(2)
      val arg3 = const(false)
      val combined: ArgDef[(String, Int, Boolean)] = arg1 & arg2 & arg3
      val next: ArgDef[(String, Int, Boolean, Long)] = combined & const(1L)
      val toTupled: ArgDef[(String, Int, Boolean, Long, String, Int, Boolean)] = next & combined
    }

  }
}

