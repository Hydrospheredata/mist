package io.hydrosphere.mist.master.execution

import akka.actor.{ActorRef, ActorSystem}
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
    val connector = successfulConnector(connectionActor.ref)

    val job = TestProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      executorStarter = (_, _) => connector,
      jobFactory = ActorF.static(job.ref)
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()

    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.executorId shouldBe None
    status.jobs.isEmpty shouldBe true

    probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "id"
        info.promise.future
    }

    job.expectMsgType[JobActor.Event.Perform]

    probe.send(frontend, ContextFrontend.Event.Status)
    val status2 = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status2.executorId.isDefined shouldBe true
    status2.jobs should contain only("id" -> ExecStatus.Started)

    job.send(frontend, JobActor.Event.Completed("id"))

    probe.send(frontend, ContextFrontend.Event.Status)
    val status3 = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status3.executorId.isDefined shouldBe true
    status3.jobs.isEmpty shouldBe true
  }

  it("should restart connector") {
//    fail("not imlemented")
  }

  it("should warmup precreated") {
//    fail("not imlemented")
  }

  it("should respect idle timeout") {
//    fail("not imlemented")
  }

  it("should release unused connections") {
//    fail("not imlemented")
  }

  it("should restart connector 'til max start times and then sleep") {
    fail("not implemented")
  }

  it("should ask connection 'til max ask times and then sleep") {
    fail("not implemented")
  }

  def successfulConnector(conn: ActorRef): WorkerConnector = {
    val connection = WorkerConnection("id", conn, workerLinkData, Promise[Unit].future)
    new WorkerConnector {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future
      override def askConnection(): Future[WorkerConnection] = Future.successful(connection)
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
      override def warmUp(): Unit = ()
    }
  }
}
