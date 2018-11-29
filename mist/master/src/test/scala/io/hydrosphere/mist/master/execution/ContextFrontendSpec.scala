package io.hydrosphere.mist.master.execution

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.common.CommonData.{Action, JobParams, RunJobRequest, _}
import io.hydrosphere.mist.common.MockitoSugar
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.PerJobConnection
import io.hydrosphere.mist.master.logging.{JobLogger, JobLoggersFactory}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.master.{ActorSpec, FilteredException, TestData, TestUtils}
import io.hydrosphere.mist.utils.akka.ActorF
import mist.api.data.JsMap
import org.mockito.Mockito.verify
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent._
import scala.concurrent.duration._

class ContextFrontendSpec extends ActorSpec("ctx-frontend-spec")
  with TestUtils
  with TestData
  with MockitoSugar
  with Eventually {

  val NOOPLoggerFactory = new JobLoggersFactory {
    override def getJobLogger(id: String): JobLogger = JobLogger.NOOP
  }

  it("should execute jobs") {
    val connector = successfulConnector()

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()

    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.executorId shouldBe None
    status.jobs.isEmpty shouldBe true

    probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))

    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "id"
        info.promise.future
    }

    job.expectMsgType[JobActor.Event.Perform]

    probe.send(frontend, ContextFrontend.Event.Status)
    val status2 = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status2.executorId.isDefined shouldBe true
    status2.jobs.size shouldBe 1
    status2.jobs.head shouldBe "id" -> ExecStatus.Started

    job.send(frontend, JobActor.Event.Completed("id"))

    probe.send(frontend, ContextFrontend.Event.Status)
    val status3 = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status3.executorId.isDefined shouldBe true
    status3.jobs.isEmpty shouldBe true
  }

  it("should respect idle timeout - awaitRequest") {
    val connector = successfulConnector()

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 1 second
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    shouldTerminate(3 seconds)(frontend)
  }

  it("should respect idle timeout - emptyConnected") {
    val connector = successfulConnector()

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 1 second
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext.copy(downtime = 1 second))

    val probe = TestProbe()
    probe.send(frontend, RunJobRequest("idx", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "idx"
        info.promise.future
    }
    job.expectMsgType[JobActor.Event.Perform]
    job.send(frontend, JobActor.Event.Completed("idx"))

    shouldTerminate(3 seconds)(frontend)
  }

  it("should release unused connections") {
    val connectionPromise = Promise[PerJobConnection]
    val connector = oneTimeConnector(connectionPromise.future)

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )

    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)
    val probe = TestProbe()
    probe.send(frontend, RunJobRequest("idx", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "idx"
        info.promise.future
    }

    probe.send(frontend, CancelJobRequest("idx"))
    job.expectMsgType[JobActor.Event.Cancel.type]
    job.send(frontend, JobActor.Event.Completed("idx"))

    val conn = mock[PerJobConnection]
    connectionPromise.success(conn)
    eventually(timeout(Span(3, Seconds))) {
      verify(conn).release()
    }
  }

  it("should restart connector 'til max start times and then sleep") {
    val callCounter = new AtomicInteger(0)
    val connector = crushedConnector()
    val starter = (id: String, ctx: ContextConfig) => {
      callCounter.incrementAndGet()
      Future.successful(connector)
    }

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = starter,
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)
    val probe = TestProbe()

    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]

    job.expectMsgType[JobActor.Event.ContextBroken]

    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.failures shouldBe TestUtils.FooContext.maxConnFailures
    callCounter.get() shouldBe TestUtils.FooContext.maxConnFailures


    probe.send(frontend, RunJobRequest(s"last", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgPF() {
      case ExecutionInfo(_, pr)=>
        intercept[FilteredException] {
          pr.future.await
        }
    }
  }

  it("should ask connection 'til max ask times and then sleep") {
    val connector = failedConnection()
    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()
    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]
    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.failures shouldBe TestUtils.FooContext.maxConnFailures

    job.expectMsgType[JobActor.Event.ContextBroken]

    probe.send(frontend, RunJobRequest(s"last", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgPF() {
      case ExecutionInfo(_, pr)=>
        intercept[FilteredException] {
          pr.future.await
        }
    }
  }

  it("should wake up after context update") {
    val connector = failedConnection()
    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()
    probe.send(frontend, RunJobRequest(s"id", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))
    probe.expectMsgType[ExecutionInfo]
    probe.send(frontend, ContextEvent.UpdateContext(TestUtils.FooContext))
    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.failures shouldBe 0
    status.jobs shouldBe Map()
  }

  it("should cancel correctly") {
    val connectionP = Promise[PerJobConnection]
    val connector = oneTimeConnector(connectionP.future)

    val job = mkJobProbe()
    val props = ContextFrontend.props(
      name = "name",
      status = StatusReporter.NOOP,
      loggersFactory = NOOPLoggerFactory,
      connectorStarter = (_, _) => Future.successful(connector),
      jobFactory = ActorF.static(job.ref),
      defaultInactiveTimeout = 5 minutes
    )
    val frontend = TestActorRef[ContextFrontend](props)
    frontend ! ContextEvent.UpdateContext(TestUtils.FooContext)

    val probe = TestProbe()

    probe.send(frontend, ContextFrontend.Event.Status)
    val status = probe.expectMsgType[ContextFrontend.FrontendStatus]
    status.executorId shouldBe None
    status.jobs.isEmpty shouldBe true

    probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, Action.Execute)))

    probe.expectMsgPF(){
      case info: ExecutionInfo =>
        info.request.id shouldBe "id"
        info.promise.future
    }

    probe.send(frontend, CancelJobRequest("id"))
    job.expectMsgType[JobActor.Event.Cancel.type]
    connectionP.success(NotImplConnection)
    job.expectNoMessage(2 seconds)
    job.send(frontend, JobActor.Event.Completed("id"))
  }

  def mkJobProbe(): TestProbe = {
    val p = TestProbe()
    p.ignoreMsg {
      case JobActor.Event.WorkerRequested => true
      case _ => false
    }
    p
  }

  object NotImplConnection extends PerJobConnection {
    override def id: String = "id"
    override def whenTerminated: Future[Unit] = Promise[Unit].future
    override def run(req: RunJobRequest, respond: ActorRef): Unit = ()
    override def cancel(id: String, respond: ActorRef): Unit = ()
    override def release(): Unit = ()
  }

  def successfulConnector(): Cluster = {
    new Cluster {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future
      override def askConnection(): Future[PerJobConnection] = Future.successful(NotImplConnection)
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }
  }

  def failedConnection():Cluster = {
    new Cluster {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future
      override def askConnection(): Future[PerJobConnection] = Future.failed(FilteredException())
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }
  }

  def crushedConnector(): Cluster = {
    new Cluster {
      override def whenTerminated(): Future[Unit] = Promise[Unit].failure(FilteredException()).future
      override def askConnection(): Future[PerJobConnection] = Promise[PerJobConnection].future
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }
  }

  def oneTimeConnector(future: Future[PerJobConnection]): Cluster = {
    new Cluster {
      override def whenTerminated(): Future[Unit] = Promise[Unit].future
      override def askConnection(): Future[PerJobConnection] = future
      override def shutdown(force: Boolean): Future[Unit] = Promise[Unit].future
    }
  }

}
