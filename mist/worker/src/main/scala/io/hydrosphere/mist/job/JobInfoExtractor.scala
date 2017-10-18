package io.hydrosphere.mist.job

import java.io.File

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.{FullJobInfo, JobsLoader}
import mist.api.UserInputArgument
import mist.api.internal.{BaseJobInstance, JavaJobInstance, JobInstance}
import org.apache.spark.util.SparkClassLoader

import scala.util.{Success, Try}

case class JobInfo(
  instance: BaseJobInstance = JobInstance.NoOpInstance,
  info: FullJobInfo
)

trait JobInfoExtractor {
  def extractInfo(file: File, className: String): Try[JobInfo]
  def extractInstance(file: File, className: String, action: Action): Try[BaseJobInstance]
}

class JvmJobInfoExtractor extends JobInfoExtractor {
  override def extractInfo(file: File, className: String): Try[JobInfo] = {
    val executeJobInstance = loadJobInstance(className, file, Action.Execute)
    val serveJobInstance = loadJobInstance(className, file, Action.Serve)
    executeJobInstance orElse serveJobInstance map { instance =>
      val lang = instance match {
        case _: JavaJobInstance => FullJobInfo.JavaLang
        case _ => FullJobInfo.ScalaLang
      }
      JobInfo(instance, FullJobInfo(
        lang = lang,
        execute = instance.describe().collect { case x: UserInputArgument => x },
        isServe = serveJobInstance.isSuccess,
        className = className
      ))
    }
  }

  override def extractInstance(file: File, className: String, action: Action): Try[BaseJobInstance] =
    loadJobInstance(className, file, action)

  private def loadJobInstance(className: String, file: File, action: Action): Try[BaseJobInstance] = {
    val loader = prepareClassloader(file)
    val jobsLoader = new JobsLoader(loader)
    jobsLoader.loadJobInstance(className, action)
  }

  private def prepareClassloader(file: File): ClassLoader = {
    val existing = this.getClass.getClassLoader
    val url = file.toURI.toURL
    val patched = SparkClassLoader.withURLs(existing, url)
    Thread.currentThread().setContextClassLoader(patched)
    patched
  }

}

class PythonJobInfoExtractor extends JobInfoExtractor {
  override def extractInfo(file: File, className: String) = Success(
    JobInfo(info = FullJobInfo(
      lang = FullJobInfo.PythonLang,
      className = className
    )))

  override def extractInstance(file: File, className: String, action: Action): Try[BaseJobInstance] =
    Success(JobInstance.NoOpInstance)
}
