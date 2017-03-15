package io.hydrosphere.mist.jobs.runners

import io.hydrosphere.mist.api.ContextWrapper
import io.hydrosphere.mist.jobs.runners.Runner.Status.Status
import io.hydrosphere.mist.jobs.runners.jar.JarRunner
import io.hydrosphere.mist.jobs.runners.python.PythonRunner
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobFile}
import io.hydrosphere.mist.utils.Logger


private[mist] trait Runner extends Logger {

  final val id: String = java.util.UUID.randomUUID.toString

  protected var _status = Runner.Status.Initialized

  /** Status getter
    *
    * @return [[Status]]
    */
  def status: Status = _status

  val configuration : FullJobConfiguration

  def run(): Either[Map[String, Any], String]

  def stop(): Unit = {
    _status = Runner.Status.Stopped
    stopStreaming()
  }

  def stopStreaming(): Unit
}

private[mist] object Runner {

  object Status extends Enumeration {
    type Status = Value
    val Initialized, Running, Stopped, Aborted = Value
  }

  def apply(configuration: FullJobConfiguration, contextWrapper: ContextWrapper): Runner = {
    val jobFile = JobFile(configuration.path)

    JobFile.fileType(configuration.path) match {
      case JobFile.FileType.Jar => new JarRunner(configuration, jobFile, contextWrapper)
      case JobFile.FileType.Python => new PythonRunner(configuration, jobFile, contextWrapper)
      case _ => throw new Exception(s"Unknown file type in ${configuration.path}")
    }

  }

}
