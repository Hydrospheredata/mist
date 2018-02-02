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
    val next2 = next.toGranted("1")
    next2.hasWaiting("1") shouldBe false
    next2.hasGranted("1") shouldBe true
    next2.hasWorking("1") shouldBe false

    val next3 = next2.toWorking("1")
    next3.hasWaiting("1") shouldBe false
    next3.hasGranted("1") shouldBe false
    next3.hasWorking("1") shouldBe true

    val next4 = next3.done("1")
    next4.hasWaiting("1") shouldBe false
    next4.hasGranted("1") shouldBe false
    next4.hasWorking("1") shouldBe false
    // waiting -> granted -> working - done

    val next5 = next4.grantNext(2)((_, _) => ())
    next5.hasGranted("2") shouldBe true
    next5.hasGranted("3") shouldBe true
    next5.hasGranted("4") shouldBe false
  }
}
