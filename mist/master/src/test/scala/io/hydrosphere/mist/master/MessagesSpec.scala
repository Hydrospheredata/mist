package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.Action
import io.hydrosphere.mist.core.CoreData.{JobParams, RunJobRequest}
import io.hydrosphere.mist.master.Messages.JobExecution.RunJobCommand
import io.hydrosphere.mist.master.models.RunMode
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
