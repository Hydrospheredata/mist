package io.hydrosphere.mist.worker.runners.scala

import java.io.File

import io.hydrosphere.mist.core.CommonData.{JobParams, RunJobRequest}
import io.hydrosphere.mist.core.jvmjob.JobsLoader
import io.hydrosphere.mist.utils.FutureOps._
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.JobRunner
import org.apache.spark.util.SparkClassLoader

import scala.concurrent.{ExecutionContext, Future}


class ScalaRunner(jobFile: File) extends JobRunner {

  override def run(
    request: RunJobRequest,
    context: NamedContext)(implicit ec: ExecutionContext): Future[Map[String, Any]] = {

    val params = request.params
    import params._

    if (!jobFile.exists()) {
      Future.failed(new IllegalArgumentException(s"Cannot find file $filePath"))
    } else {
      context.addJar(params.filePath)
      val loader = prepareClassloader(jobFile)
      val jobsLoader = new JobsLoader(loader)
      for {
        instance <- Future.fromTry(jobsLoader.loadJobInstance(className, action))
        setupConf = context.setupConfiguration(request.id)
        result <- Future.fromEither(instance.run(setupConf, arguments))
      } yield result
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

}
