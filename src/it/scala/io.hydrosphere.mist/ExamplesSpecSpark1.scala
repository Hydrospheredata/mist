package io.hydrosphere.mist

import org.scalatest.{Ignore, Matchers, FunSpec}

class ExamplesSpecSpark1 extends FunSpec with MistItTest with Matchers {

  override val configPath: String = "examples-spark1/integration.conf"

  if (isSpark1) {
    val interface = MistHttpInterface("localhost", 2004)

    it("run simple context") {
      val result = interface.runJob("simple-context",
        Map(
          "numbers" -> List(1, 2, 3),
          "multiplier" -> 2))

      result.success shouldBe true
    }
  }

}
