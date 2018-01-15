package io.hydrosphere.mist

import org.scalatest.{FunSpec, Matchers}

class ExamplesSpecSpark extends FunSpec with MistItTest with Matchers {

  val interface = MistHttpInterface("localhost", 2004)

  it("spark-ctx-example") {
    val result = interface.runJob("spark-ctx-example",
        "numbers" -> List(1, 2, 3),
        "multiplier" -> 2
    )

    result.success shouldBe true
  }
}
