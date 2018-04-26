package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Path

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.execution.workers.StopAction

import scala.concurrent.Future
import scala.language.postfixOps

sealed trait WorkerProcess
case object NonLocal extends WorkerProcess
case class Local(termination: Future[Unit]) extends WorkerProcess

trait WorkerStarter {

  def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess

  def stopAction: StopAction
}

object WorkerStarter {

  def create(workersSettings: WorkersSettingsConfig, outDirectory: Path): WorkerStarter = {
    val runnerType = workersSettings.runner
    runnerType match {
      case "local" => LocalSparkSubmit(outDirectory)
      case "docker" => DockerStarter(workersSettings.dockerConfig)
      case "manual" => ManualStarter(workersSettings.manualConfig, outDirectory)
      case _ => throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")
    }
  }
}

