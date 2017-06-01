package io.hydrosphere.mist.worker.runners.scala

import java.io.File

import cats.implicits._
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.jar.JobsLoader
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.JobRunner

class ScalaRunner extends JobRunner {

  override def run(
    params: JobParams,
    context: NamedContext): Either[String, Map[String, Any]] = {

    import params._

    val file = new File(filePath)
    if (!file.exists()) {
      Left(s"Can not found file: $filePath")
    } else {
      context.addJar(params.filePath)
      val jobsLoader = JobsLoader.fromJar(file)
      // see #204
      Thread.currentThread().setContextClassLoader(jobsLoader.classLoader)

      val load = jobsLoader.loadJobInstance(className, action)
      Either.fromTry(load).flatMap(instance => {
        instance.run(context.setupConfiguration, arguments)
      }).leftMap(e => buildErrorMessage(params, e))
    }
  }

  private def buildErrorMessage(params: JobParams, e: Throwable): String = {
    val msg = Option(e.getMessage).getOrElse("")
    val trace = e.getStackTrace.map(e => e.toString).mkString("; ")
    s"Error running job with $params. Type: ${e.getClass.getCanonicalName}, message: $msg, trace $trace"
  }

}
