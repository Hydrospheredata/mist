package io.hydrosphere.mist.jobs.runners.jar

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.utils.TypeAlias.{JobResponse, JobResponseOrError}
import io.hydrosphere.mist.utils.{ExternalInstance, ExternalJar}

class JarRunner(
  override val job: JobDetails,
  jobFile: JobFile,
  context: NamedContext) extends Runner {

  // TODO: remove nullable contextWrapper
  if (context!= null) {
    // We must add user jar into spark context
    context.addJar(jobFile.file.getPath)
  }

  val externalInstance: ExternalInstance = ExternalJar(jobFile.file.getAbsolutePath)
    .getExternalClass(job.configuration.className)
    .getNewInstance

  override def run(): JobResponseOrError = {
    try {
      val result = job.configuration.action match {
        case JobConfiguration.Action.Execute =>
          externalInstance.objectRef.asInstanceOf[MistJob].setup(context.setupConfiguration)
          Left(externalInstance.getMethod("execute").run(job.configuration.parameters).asInstanceOf[JobResponse])
        case JobConfiguration.Action.Train =>
          externalInstance.objectRef.asInstanceOf[MLMistJob].setup(context.setupConfiguration)
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
