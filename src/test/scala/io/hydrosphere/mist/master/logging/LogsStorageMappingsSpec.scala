package io.hydrosphere.mist.master.logging

import java.nio.file.Paths

import org.scalatest._

class LogsStorageMappingsSpec extends FunSpec with Matchers {

  it("should return path to logs") {
    val mappings = new LogStorageMappings(Paths.get("dir"))
    mappings.pathFor("ascf") shouldBe Paths.get("dir", "job-ascf.log")
  }

  it("shouldn't accept insecure id") {
    val mappings = new LogStorageMappings(Paths.get("dir"))
    intercept[Throwable] {
      mappings.pathFor("../../password")
    }
  }
}
