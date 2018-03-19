package io.hydrosphere.mist.master.execution

import org.scalatest._

class FrontendStateSpec extends FunSpec with Matchers {

  it("should behave correctly") {
    val state = FrontendState(
      "1" -> 1,
      "2" -> 2,
      "3" -> 3
    )

    state.hasWaiting("1") shouldBe true
    state.get("1") shouldBe Some(1)

    val next = state.enqueue("4", 4)
    next.get("4") shouldBe Some(4)
    next.get("5") shouldBe None

    // waiting -> granted -> working - done
    val next2 = next.toWorking("1")
    next2.hasWaiting("1") shouldBe false
    next2.hasWorking("1") shouldBe true

    val next3 = next2.done("1")
    next3.hasWaiting("1") shouldBe false
    next3.hasWorking("1") shouldBe false
    // waiting -> granted -> working - done

    next3.nextOption shouldBe Some("2" -> 2)
  }
}
