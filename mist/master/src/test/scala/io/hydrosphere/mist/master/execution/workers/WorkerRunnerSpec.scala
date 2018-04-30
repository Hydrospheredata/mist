package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicBoolean

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.execution.workers.starter.{WorkerProcess, WorkerStarter}
import io.hydrosphere.mist.master.execution.{SpawnSettings, workers}
import io.hydrosphere.mist.master.{ActorSpec, FilteredException, TestData}
import io.hydrosphere.mist.utils.akka.ActorRegHub
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class WorkerRunnerSpec extends ActorSpec("worker-runner") with TestData with MockitoSugar with Eventually {

  describe("default runner") {

    def mkSpawnSettings(starter: WorkerStarter): SpawnSettings = SpawnSettings(
      runnerCmd = starter,
      timeout = 10 seconds,
      readyTimeout = 10 seconds,
      akkaAddress = "akkaAddr",
      logAddress = "logAddr",
      httpAddress = "httpAddr",
      maxArtifactSize = 100L
    )

    it("should run worker") {
      val starter = new WorkerStarter {
        override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = WorkerProcess.NonLocal
        override def stopAction: StopAction = StopAction.Remote
      }
      val regHub = mock[ActorRegHub]
      when(regHub.waitRef(any[String], any[Duration])).thenSuccess(null)

      val termination = Promise[Unit]
      val runner = new workers.WorkerRunner.DefaultRunner(
        spawn = mkSpawnSettings(starter),
        regHub = regHub,
        connect = (_, _, _, _, _) => Future.successful(WorkerConnection("id", null, workerLinkData, termination.future))
      )

      Await.result(runner("id", FooContext), Duration.Inf)
    }

    it("should call onStop if connect was failed") {
      val check = new AtomicBoolean(false)

      val runnerCmd = new WorkerStarter {
        override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = WorkerProcess.NonLocal
        override def stopAction: StopAction = StopAction.CustomFn(_ => check.set(true))
      }

      val regHub = mock[ActorRegHub]
      when(regHub.waitRef(any[String], any[Duration])).thenFailure(FilteredException())

      val runner = new workers.WorkerRunner.DefaultRunner(
        spawn = mkSpawnSettings(runnerCmd),
        regHub = regHub,
        connect = (_, _, _, _, _) => Future.failed(FilteredException())
      )

      intercept[Throwable] {
        Await.result(runner("id", FooContext), Duration.Inf)
      }

      eventually(timeout(Span(3, Seconds))) {
        check.get shouldBe true
      }
    }

  }
}
