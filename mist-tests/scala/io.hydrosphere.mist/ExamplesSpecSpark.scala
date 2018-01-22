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

  it("should run py simple context") {
    val result = interface.runJob("simple-context-py",
      "numbers" -> List(1, 2, 3)
    )
    assert(result.success, s"Job is failed $result")
  }

  it("should run hive job") {
    val result = interface.runJob("hive-ctx-py",
      "path" -> "./mist-tests/resources/pyjobs/jobs/hive_job_data.json"
    )
    assert(result.success, s"Job is failed $result")
  }

}
