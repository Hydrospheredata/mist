package io.hydrosphere.mist.jobs.runners.jar

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.utils.{ExternalInstance, ExternalJar}

private[mist] class JarRunner(override val configuration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {

  // TODO: remove nullable contextWrapper
  if (contextWrapper != null) {
    // We must add user jar into spark context
    contextWrapper.addJar(jobFile.file.getPath)
  }

  val externalInstance: ExternalInstance = ExternalJar(jobFile.file.getAbsolutePath)
    .getExternalClass(configuration.className)
    .getNewInstance

  _status = Runner.Status.Initialized

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
    try {
      val result = configuration match {
        case _: MistJobConfiguration =>
          externalInstance.objectRef.asInstanceOf[MistJob].setup(contextWrapper)
          Left(externalInstance.getMethod("execute").run(configuration.parameters).asInstanceOf[Map[String, Any]])
        case _: TrainingJobConfiguration =>
          externalInstance.objectRef.asInstanceOf[MLMistJob].setup(contextWrapper)
          Left(externalInstance.getMethod("train").run(configuration.parameters).asInstanceOf[Map[String, Any]])
        case _: ServingJobConfiguration =>
          Left(externalInstance.getMethod("serve").run(configuration.parameters).asInstanceOf[Map[String, Any]])
      }

      _status = Runner.Status.Stopped

      result
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = Runner.Status.Aborted
        Right(e.toString)
    }
  }

  override def stopStreaming(): Unit = { 
    if (externalInstance.externalClass.isStreamingJob)
      externalInstance.objectRef.asInstanceOf[StreamingSupport].stopStreaming()
  }
}
