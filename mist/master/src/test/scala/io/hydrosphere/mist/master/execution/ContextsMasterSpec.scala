package io.hydrosphere.mist.master.execution

import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.execution.ContextEvent.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.{ActorSpec, TestData}
import io.hydrosphere.mist.utils.akka.ActorF

class ContextsMasterSpec extends ActorSpec("contexts-master") with TestData {

  it("should spawn/ proxy to contexts") {
    val ctx = TestProbe()
    val master = TestActorRef[ContextsMaster](ContextsMaster.props(
      contextF = ActorF.static(ctx.ref)
    ))

    master ! RunJobCommand(FooContext, mkRunReq("id"))
    ctx.expectMsgType[RunJobRequest]

    master ! CancelJobCommand(FooContext.name, CancelJobRequest("id"))
    ctx.expectMsgType[CancelJobRequest]
  }
}
