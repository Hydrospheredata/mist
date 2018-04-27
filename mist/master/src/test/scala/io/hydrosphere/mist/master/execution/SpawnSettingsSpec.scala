package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.master.TestData
import io.hydrosphere.mist.master.execution.workers.StopAction
import io.hydrosphere.mist.master.execution.workers.starter.{WorkerProcess, WorkerStarter}
import io.hydrosphere.mist.master.models.RunMode
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class SpawnSettingsSpec extends FunSpec with Matchers with TestData {

  it("should build worker init info") {
    val noop = new WorkerStarter {
      override def onStart(name: String, initInfo: CommonData.WorkerInitInfo): WorkerProcess = WorkerProcess.NonLocal
      override def stopAction: StopAction = StopAction.Remote
    }
    val spawnSettings = SpawnSettings(
      runnerCmd = noop,
      timeout = 10 seconds,
      readyTimeout = 10 seconds,
      akkaAddress = "akkaAddr",
      logAddress = "logAddr",
      httpAddress = "httpAddr",
      maxArtifactSize = 100L
    )

    val ctx = FooContext.copy(workerMode = RunMode.Shared)
    val initInfo = spawnSettings.toWorkerInitInfo(ctx)
    initInfo.sparkConf.toSeq should contain allElementsOf ctx.sparkConf.toSeq
    initInfo.maxJobs shouldBe ctx.maxJobs
    initInfo.downtime shouldBe ctx.downtime
    initInfo.streamingDuration shouldBe  ctx.streamingDuration
    initInfo.logService shouldBe spawnSettings.logAddress
    initInfo.masterHttpConf shouldBe spawnSettings.httpAddress
    initInfo.maxArtifactSize shouldBe spawnSettings.maxArtifactSize

    val excl = ctx.copy(workerMode = RunMode.ExclusiveContext)
    val initFoExclusive = spawnSettings.toWorkerInitInfo(excl)
    initFoExclusive.maxJobs shouldBe 1
  }
}
