package io.hydrosphere.mist.master.execution

import org.scalatest._

class FrontendStateSpec extends FunSpec with Matchers {

  it("should behave correctly") {
    val state = FrontendState(
      "1" -> 1,
      "2" -> 2,
      "3" -> 3
    )

    state.get("1") shouldBe Some(1)

    val next = state.enqueue("4", 4)

    next.get("4") shouldBe Some(4)
    next.get("5") shouldBe None

    state.hasQueued("1") shouldBe true

    val next2 = state.removeFromQueue("1")
    next2.hasQueued("1") shouldBe false
    next2.has("1") shouldBe true

    next2.take(2) shouldBe Map("2" -> 2, "3" -> 3)
  }
}
