package io.hydrosphere.mist.worker.runners

import java.io.File

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.core.jvmjob.JobsLoader
import io.hydrosphere.mist.utils.EitherOps._
import io.hydrosphere.mist.worker.NamedContext
import mist.api.FnContext
import mist.api.data.JsLikeData
import org.apache.spark.util.SparkClassLoader

import scala.util.{Failure, Success}


class ScalaRunner(jobFile: File) extends JobRunner {

  override def run(
    request: RunJobRequest,
    context: NamedContext):Either[Throwable, JsLikeData] = {

    val params = request.params
    import params._

    if (!jobFile.exists()) {
      Left(new IllegalArgumentException(s"Cannot find file $filePath"))
    } else {
      context.addJar(jobFile.toString)
      val loader = prepareClassloader(jobFile)
      val jobsLoader = new JobsLoader(loader)
      val instance = jobsLoader.loadJobInstance(className, action) match {
        case Success(i) => Right(i)
        case Failure(ex) => Left(ex)
      }
      for {
        inst      <- instance
        setupConf =  context.setupConfiguration(request.id)
        _         <- inst.validateParams(params.arguments)
        result    <- inst.run(FnContext(setupConf, params.arguments))
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
