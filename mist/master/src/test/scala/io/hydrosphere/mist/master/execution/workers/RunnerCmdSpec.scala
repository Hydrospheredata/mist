package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master._
import org.scalatest.{FunSpec, Matchers}

class RunnerCmdSpec extends FunSpec with Matchers with TestData {

  describe("Shell args") {

    it("should build arguments for worker") {
      val name = "worker-name"
      val context = FooContext
      val result = ShellWorkerScript.workerArgs(name, context, "0.0.0.0:2345")
        val x = Seq(
        "--master", "0.0.0.0:2345",
        "--name", "worker-name",
        "--run-options", "--opt"
      )
      result should contain theSameElementsAs x
    }
  }
}
