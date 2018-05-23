package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.worker.SparkArtifact
import io.hydrosphere.mist.worker.runners.python.PythonRunner

trait RunnerSelector {
  def selectRunner(artifact: SparkArtifact): JobRunner
}

class SimpleRunnerSelector extends RunnerSelector {

  override def selectRunner(artifact: SparkArtifact): JobRunner = artifact.fileExt match {
    case "py" | "egg"  => new PythonRunner(artifact)
    case "jar" => new ScalaRunner(artifact)
    case f => throw new IllegalArgumentException(s"Unknown file type $f is passed, cannot select runner")
  }

}
