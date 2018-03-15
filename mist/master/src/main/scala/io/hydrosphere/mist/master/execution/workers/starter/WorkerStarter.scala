package io.hydrosphere.mist.master.execution.workers.starter

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master._

import scala.concurrent.Future
import scala.language.postfixOps

sealed trait WorkerProcess
case object NonLocal extends WorkerProcess
case class Local(termination: Future[Unit]) extends WorkerProcess

trait WorkerStarter {

  def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess

  def onStop(name: String): Unit = {}
}

object WorkerStarter {

  def create(workersSettings: WorkersSettingsConfig): WorkerStarter = {
    val runnerType = workersSettings.runner
    runnerType match {
      case "local" => LocalSparkSubmit()
      case "docker" => DockerStarter(workersSettings.dockerConfig)
      case "manual" => ManualStarter(workersSettings.manualConfig)
      case _ => throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

