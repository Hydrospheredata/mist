package io.hydrosphere.mist.jobs.runners

import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.runners.jar.JarRunner
import io.hydrosphere.mist.jobs.runners.python.PythonRunner
import io.hydrosphere.mist.jobs.{JobDetails, JobFile}
import io.hydrosphere.mist.utils.Logger


private[mist] trait Runner extends Logger {

  val job: JobDetails

  def run(): Either[Map[String, Any], String]

  def stop(): Unit = {
    stopStreaming()
  }

  def stopStreaming(): Unit
}

private[mist] object Runner {

  def apply(job: JobDetails, contextWrapper: ContextWrapper): Runner = {
    val jobFile = JobFile(job.configuration.path)

    JobFile.fileType(job.configuration.path) match {
      case JobFile.FileType.Jar => new JarRunner(job, jobFile, contextWrapper)
      case JobFile.FileType.Python => new PythonRunner(job, jobFile, contextWrapper)
      case _ => throw new Exception(s"Unknown file type in ${job.configuration.path}")
    }

  }

}
