package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.{ActorSpec, FilteredException, TestData}
import io.hydrosphere.mist.master.execution.{SpawnSettings, workers}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.ActorRegHub
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class WorkerRunnerSpec extends ActorSpec("worker-runner") with TestData with MockitoSugar with Eventually {

  describe("default runner") {

    def mkSpawnSettings(cmd: RunnerCmd): SpawnSettings = SpawnSettings(
      runnerCmd = cmd,
      timeout = 10 seconds,
      readyTimeout = 10 seconds,
      akkaAddress = "akkaAddr",
      logAddress = "logAddr",
      httpAddress = "httpAddr",
      maxArtifactSize = 100L,
      jobsSavePath = "/tmp"
    )

    it("should run worker") {
      val runnerCmd = mock[RunnerCmd]
      val regHub = mock[ActorRegHub]
      when(regHub.waitRef(any[String], any[Duration])).thenSuccess(null)

      val termination = Promise[Unit]
      val runner = new workers.WorkerRunner.DefaultRunner(
        spawn = mkSpawnSettings(runnerCmd),
        regHub = regHub,
        connect = (_, _, _, _) => Future.successful(WorkerConnection("id", null, workerLinkData, termination.future))
      )

      Await.result(runner("id", FooContext), Duration.Inf)
      verify(runnerCmd).runWorker(any[String], any[ContextConfig])

      termination.success(())

      eventually(timeout(Span(3, Seconds))) {
        verify(runnerCmd).onStop(any[String])
      }
    }

    it("should call onStop if await ref was failed") {
      val runnerCmd = mock[RunnerCmd]
      val regHub = mock[ActorRegHub]
      when(regHub.waitRef(any[String], any[Duration])).thenFailure(FilteredException())

      val runner = new workers.WorkerRunner.DefaultRunner(
        spawn = mkSpawnSettings(runnerCmd),
        regHub = regHub,
        connect = (_, _, _, _) => Future.failed(FilteredException())
      )

      intercept[Throwable] {
        Await.result(runner("id", FooContext), Duration.Inf)
      }

      eventually(timeout(Span(3, Seconds))) {
        verify(runnerCmd).onStop(any[String])
      }
    }

    it("should call onStop if connect was failed") {
      val runnerCmd = mock[RunnerCmd]
      val regHub = mock[ActorRegHub]
      when(regHub.waitRef(any[String], any[Duration])).thenSuccess(null)

      val runner = new workers.WorkerRunner.DefaultRunner(
        spawn = mkSpawnSettings(runnerCmd),
        regHub = regHub,
        connect = (_, _, _, _) => Future.failed(FilteredException())
      )

      intercept[Throwable] {
        Await.result(runner("id", FooContext), Duration.Inf)
      }

      eventually(timeout(Span(3, Seconds))) {
        verify(runnerCmd).onStop(any[String])
      }
    }
  }
}
