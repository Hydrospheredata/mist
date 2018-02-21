package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master.TestData
import io.hydrosphere.mist.master.models.ContextConfig
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class WorkerHubSpec extends FunSpec with Matchers with TestData with Eventually {

  it("should mirror connections") {
    val termination = Promise[Unit]

    val runner = new WorkerRunner {
      override def apply(id: String, ctx: ContextConfig): Future[WorkerConnection] =
        Future.successful(WorkerConnection(id, null, workerLinkData.copy(name = id), termination.future))
    }
    val hub = new WorkerHub(runner, TestConnector.apply)

    val connector = hub.start("id", FooContext)

    Await.result(connector.askConnection(), Duration.Inf)

    eventually(timeout(Span(3, Seconds))) {
      hub.workerConnections().size shouldBe 1
    }
    termination.success(())
    eventually(timeout(Span(3, Seconds))) {
      hub.workerConnections().size shouldBe 0
    }
  }


  case class TestConnector(
    id: String,
    ctx: ContextConfig,
    runner: WorkerRunner) extends WorkerConnector {

    def askConnection(): Future[WorkerConnection] = runner(id, ctx)

    def warmUp(): Unit = ()

    def shutdown(force: Boolean): Future[Unit] = ???

    def whenTerminated(): Future[Unit] = ???
  }
}
