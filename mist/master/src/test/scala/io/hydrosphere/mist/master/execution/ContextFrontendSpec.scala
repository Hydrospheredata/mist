package io.hydrosphere.mist.master.execution

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.master.TestUtils
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.{WorkerConnection, WorkerConnector}
import io.hydrosphere.mist.utils.akka.ActorF
import io.hydrosphere.mist.master.TestData
import org.scalatest._

import scala.concurrent._

class ContextFrontendSpec extends TestKit(ActorSystem("ctx-frontend-spec"))
  with FunSpecLike
  with Matchers
  with TestUtils
  with TestData {

  it("should execute jobs") {
    val connectionActor = TestProbe()
    val connection = WorkerConnection("id", connectionActor.ref, workerLinkData, Promise[Unit].future)

    val connector = new WorkerConnector {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future

      override def askConnection(): Future[WorkerConnection] = Future.successful(connection)

      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }

    val job = TestProbe()

    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      executorStarter = (_, _) => connector,
      jobFactory = ActorF.static(job.ref)
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextFrontend.Event.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()
    probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "id"
        info.promise.future
    }

    job.expectMsgType[JobActor.Event.Perform]
  }

}
