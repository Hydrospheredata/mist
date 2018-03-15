package io.hydrosphere.mist.master.execution.workers.starter

import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WrappedProcessSpec extends FunSpec with Matchers {

  it("should return success") {
    val f = WrappedProcess.run(Seq("/bin/bash", "-c", "exit 0")).await()
    Await.result(f, Duration.Inf)
  }

  it("should fail") {
    intercept[Throwable] {
      val f = WrappedProcess.run(Seq("/bin/bash", "-c", "exit 1")).await()
      Await.result(f, Duration.Inf)
    }
  }
}
