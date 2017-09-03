package io.hydrosphere.mist.worker.runners.scala

import java.io.File

import io.hydrosphere.mist.core.CoreData.{JobParams, RunJobRequest}
import io.hydrosphere.mist.core.jvmjob.JobsLoader

import io.hydrosphere.mist.utils.EitherOps._

import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.JobRunner
import org.apache.spark.util.SparkClassLoader


class ScalaRunner(jobFile: File) extends JobRunner {

  override def run(
    request: RunJobRequest,
    context: NamedContext): Either[String, Map[String, Any]] = {

    val params = request.params
    import params._

    if (!jobFile.exists()) {
      Left(s"Can not found file: $filePath")
    } else {
      context.addJar(params.filePath)
      val loader = prepareClassloader(jobFile)
      val jobsLoader = new JobsLoader(loader)

      val load = jobsLoader.loadJobInstance(className, action)
      Either.fromTry(load).flatMap(instance => {
        instance.run(context.setupConfiguration(request.id), arguments)
      }).leftMap(e => buildErrorMessage(params, e))
    }
  }

  // see #204, #220
  private def prepareClassloader(file: File): ClassLoader = {
    val existing = this.getClass.getClassLoader
    val url = file.toURI.toURL
    val patched = SparkClassLoader.withURLs(existing, url)
    Thread.currentThread().setContextClassLoader(patched)
    patched
  }

  private def buildErrorMessage(params: JobParams, e: Throwable): String = {
    val msg = Option(e.getMessage).getOrElse("")
    val trace = e.getStackTrace.map(e => e.toString).mkString("; ")
    s"Error running job with $params. Type: ${e.getClass.getCanonicalName}, message: $msg, trace $trace"
  }

}
