package io.hydrosphere.mist.jobs.runners.jar

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.lib.{MLMistJob, MistJob, StreamingSupport}
import io.hydrosphere.mist.utils.TypeAlias.{JobResponse, JobResponseOrError}
import io.hydrosphere.mist.utils.{ExternalInstance, ExternalJar}

private[mist] class JarRunner(override val job: JobDetails, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {

  // TODO: remove nullable contextWrapper
  if (contextWrapper != null) {
    // We must add user jar into spark context
    contextWrapper.addJar(jobFile.file.getPath)
  }

  val externalInstance: ExternalInstance = ExternalJar(jobFile.file.getAbsolutePath)
    .getExternalClass(job.configuration.className)
    .getNewInstance

  override def run(): JobResponseOrError = {
    try {
      val result = job.configuration.action match {
        case JobConfiguration.Action.Execute =>
          externalInstance.objectRef.asInstanceOf[MistJob].setup(contextWrapper)
          Left(externalInstance.getMethod("execute").run(job.configuration.parameters).asInstanceOf[JobResponse])
        case JobConfiguration.Action.Train =>
          externalInstance.objectRef.asInstanceOf[MLMistJob].setup(contextWrapper)
          Left(externalInstance.getMethod("train").run(job.configuration.parameters).asInstanceOf[JobResponse])
        case JobConfiguration.Action.Serve =>
          Left(externalInstance.getMethod("serve").run(job.configuration.parameters).asInstanceOf[JobResponse])
      }

      result
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        Right(e.toString)
    }
  }

  override def stopStreaming(): Unit = { 
    if (externalInstance.externalClass.isStreamingJob)
      externalInstance.objectRef.asInstanceOf[StreamingSupport].stopStreaming()
  }
}
