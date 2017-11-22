package io.hydrosphere.mist.job

import java.io.File

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.{FullJobInfo, JobsLoader}
import mist.api.UserInputArgument
import mist.api.internal.{BaseJobInstance, JavaJobInstance, JobInstance}
import org.apache.commons.io.FilenameUtils
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

class JvmJobInfoExtractor(jobsLoader: File => JobsLoader) extends JobInfoExtractor {

  override def extractInfo(file: File, className: String): Try[JobInfo] = {
    val executeJobInstance = extractInstance(file, className, Action.Execute)
    executeJobInstance orElse extractInstance(file, className, Action.Serve) map { instance =>
      val lang = instance match {
        case _: JavaJobInstance => FullJobInfo.JavaLang
        case _ => FullJobInfo.ScalaLang
      }

      JobInfo(instance, FullJobInfo(
        lang = lang,
        execute = instance.describe().collect { case x: UserInputArgument => x },
        isServe = !executeJobInstance.isSuccess,
        className = className,
        tags = instance.tags()
      ))
    }
  }

  override def extractInstance(file: File, className: String, action: Action): Try[BaseJobInstance] = {
    jobsLoader(file).loadJobInstance(className, action)
  }

}

object JvmJobInfoExtractor {

  def apply(): JvmJobInfoExtractor = new JvmJobInfoExtractor(file => {
    val loader = prepareClassloader(file)
    new JobsLoader(loader)
  })

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

class BaseJobInfoExtractor(
  jvmJobInfoExtractor: JvmJobInfoExtractor,
  pythonJobInfoExtractor: PythonJobInfoExtractor
) extends JobInfoExtractor {

  override def extractInfo(file: File, className: String): Try[JobInfo] =
    selectExtractor(file)
      .extractInfo(file, className)

  override def extractInstance(file: File, className: String, action: Action): Try[BaseJobInstance] =
    selectExtractor(file)
      .extractInstance(file, className, action)

  private def selectExtractor(file: File): JobInfoExtractor =
    FilenameUtils.getExtension(file.getAbsolutePath) match {
      case "py" => pythonJobInfoExtractor
      case "jar" => jvmJobInfoExtractor
    }
}

object JobInfoExtractor {
  def apply(): BaseJobInfoExtractor =
    new BaseJobInfoExtractor(JvmJobInfoExtractor(), new PythonJobInfoExtractor)
}