package io.hydrosphere.mist

import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.Messages.WorkerMessages.RunJobCommand
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.master.TestUtils
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import org.scalatest.{FunSpec, Matchers}

class MessagesSpec extends FunSpec with Matchers {

  describe("RunJobCommand") {

    it("should compute workerId") {
      val context = TestUtils.contextSettings.default.copy(name = "my-context")
      def mk(mode: RunMode): RunJobCommand = RunJobCommand(
        context = context,
        mode = mode,
        RunJobRequest("id-1", JobParams("path", "class", Map.empty, Action.Execute))
      )

      mk(RunMode.Shared).computeWorkerId() shouldBe "my-context"
      mk(RunMode.ExclusiveContext(None)).computeWorkerId() shouldBe "my-context-id-1"
      mk(RunMode.ExclusiveContext(Some("marker"))).computeWorkerId() shouldBe "my-context-marker-id-1"
    }

  }
}
