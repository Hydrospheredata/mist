package io.hydrosphere.mist.worker.runners

import java.io.File
import java.net.URL

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.job.FunctionInstanceLoader
import io.hydrosphere.mist.utils.EitherOps._
import io.hydrosphere.mist.utils.{Err, Succ}
import io.hydrosphere.mist.worker.{MistScContext, SparkArtifact}
import mist.api.{FnContext, RuntimeJobInfo}
import mist.api.data.JsData
import org.apache.spark.util.SparkClassLoader


class ScalaRunner(artifact: SparkArtifact) extends JobRunner {

  override def run(
    request: RunJobRequest,
    context: MistScContext):Either[Throwable, JsData] = {

    val params = request.params
    import params._

    context.addJar(artifact)
    val loader = prepareClassloader(artifact.local)
    val jobsLoader = new FunctionInstanceLoader(loader)
    val instance = jobsLoader.loadFnInstance(className, action) match {
      case Succ(i) => Right(i)
      case Err(ex) => Left(ex)
    }
    for {
      inst      <- instance
      ctx       =  FnContext(context.sc, params.arguments, context.streamingDuration, RuntimeJobInfo(request.id, context.namespace))
      result    <- inst.run(ctx)
    } yield result
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
