package io.hydrosphere.mist.master.execution

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.master.{FilteredException, TestData, TestUtils}
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.{WorkerConnection, WorkerConnector}
import io.hydrosphere.mist.utils.akka.ActorF
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
    val connector = crushedConnector()
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

    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", Map.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]

    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.connectorFails shouldBe ContextFrontend.ConnectorFailedMaxTimes

    probe.send(frontend, RunJobRequest(s"last", JobParams("path", "MyClass", Map.empty, Action.Execute)))
    probe.expectMsgPF() {
      case ExecutionInfo(_, pr)=>
        intercept[FilteredException] {
          pr.future.await
        }
    }
  }

  it("should ask connection 'til max ask times and then sleep") {
    val connector = failedConnection()
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
    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", Map.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]
    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.connFails shouldBe ContextFrontend.ConnectionFailedMaxTimes

    probe.send(frontend, RunJobRequest(s"last", JobParams("path", "MyClass", Map.empty, Action.Execute)))
    probe.expectMsgPF() {
      case ExecutionInfo(_, pr)=>
        intercept[FilteredException] {
          pr.future.await
        }
    }
  }
  it("should wake up after context update") {
    val connector = failedConnection()
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
    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", Map.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]
    probe.send(frontend, ContextEvent.UpdateContext(TestUtils.FooContext))
    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.connectorFails shouldBe 0
    status.connFails shouldBe 0
    status.jobs shouldBe Map()
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

  def failedConnection():WorkerConnector = {
    new WorkerConnector {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future
      override def askConnection(): Future[WorkerConnection] = Future.failed(FilteredException())
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
      override def warmUp(): Unit = ()
    }
  }

  def crushedConnector(): WorkerConnector = {
    new WorkerConnector {
      override def whenTerminated(): Future[Unit] = Promise[Unit].failure(FilteredException()).future
      override def askConnection(): Future[WorkerConnection] = Promise[WorkerConnection].future
      override def warmUp(): Unit = ()
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }
  }

}
