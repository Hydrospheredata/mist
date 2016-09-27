package io.hydrosphere.mist.jobs.runners

import io.hydrosphere.mist.Logger
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.{JobConfiguration, JobFile}
import io.hydrosphere.mist.jobs.runners.Runner.Status.Status
import io.hydrosphere.mist.jobs.runners.jar.JarRunner
import io.hydrosphere.mist.jobs.runners.python.PythonRunner


private[mist] trait Runner extends Logger {

  final val id = java.util.UUID.randomUUID.toString

  protected var _status = Runner.Status.Initialized

  /** Status getter
    *
    * @return [[Status]]
    */
  def status: Status = _status

  val configuration : JobConfiguration

  def run(): Either[Map[String, Any], String]

}

private[mist] object Runner {

  object Status extends Enumeration {
    type Status = Value
    val Initialized, Running, Stopped, Aborted = Value
  }

  def apply(configuration: JobConfiguration, contextWrapper: ContextWrapper): Runner = {
    val jobFile = JobFile(configuration.path)
    jobFile.fileType match {
      case JobFile.FileType.Jar => new JarRunner(configuration, jobFile, contextWrapper)
      case JobFile.FileType.Python => new PythonRunner(configuration, jobFile, contextWrapper)
      case _ => throw new Exception(s"Unknown file type in ${configuration.path}")
    }

  }

}