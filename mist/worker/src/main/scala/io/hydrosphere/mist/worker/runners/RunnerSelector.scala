package io.hydrosphere.mist.worker.runners

import java.io.File

import io.hydrosphere.mist.worker.runners.python.PythonRunner
import io.hydrosphere.mist.worker.runners.scala.ScalaRunner

trait RunnerSelector {
  def selectRunner(file: File): JobRunner
}

class SimpleRunnerSelector extends RunnerSelector {

  override def selectRunner(file: File): JobRunner = file.getName match {
    case fname if fname.endsWith(".py") => new PythonRunner(file)
    case fname if fname.endsWith(".jar") => new ScalaRunner(file)
    case f => throw new IllegalArgumentException(s"Unknown file type $f is passed, cannot select runner")
  }

}
