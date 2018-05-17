package io.hydrosphere.mist.master.execution.workers.starter

import org.scalatest.{FunSpec, Matchers}

class PsUtilSpec extends FunSpec with Matchers {

  import PsUtil._

  it("should parse") {
    parseArguments("echo \"YOYO\" ") should contain theSameElementsInOrderAs(Seq(
      "echo",
      "YOYO"
    ))
    parseArguments(" echo 'YOYO' abdc") should contain theSameElementsInOrderAs(Seq(
      "echo",
      "YOYO",
      "abdc"
    ))
    parseArguments("") shouldBe Seq.empty
  }
}
