package io.hydrosphere.mist.jobs.runners

import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.runners.jar.JarRunner
import io.hydrosphere.mist.jobs.runners.python.PythonRunner
import io.hydrosphere.mist.jobs.{JobDetails, JobFile}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError


trait Runner extends Logger {

  val job: JobDetails

  def run(): JobResponseOrError

  def stop(): Unit

}

private[mist] object Runner {

  def apply(job: JobDetails, context: NamedContext): Runner = {
    val jobFile = JobFile(job.configuration.path)

    JobFile.fileType(job.configuration.path) match {
      case JobFile.FileType.Jar => new JarRunner(job, jobFile, context)
      case JobFile.FileType.Python => new PythonRunner(job, jobFile, context)
      case _ => throw new Exception(s"Unknown file type in ${job.configuration.path}")
    }

  }

}
