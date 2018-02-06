package io.hydrosphere.mist.job

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.{ExtractedData, JobInfoData, JobsLoader}
import io.hydrosphere.mist.utils.{Err, Succ, TryLoad}
import mist.api.args.{InternalArgument, UserInputArgument}
import mist.api.internal.{BaseJobInstance, JavaJobInstance, JobInstance}
import org.apache.commons.io.FilenameUtils

case class JobInfo(
  instance: BaseJobInstance = JobInstance.NoOpInstance,
  data: ExtractedData
)

trait JobInfoExtractor {
  def extractInfo(file: File, className: String): TryLoad[JobInfo]
}

class JvmJobInfoExtractor(mkLoader: ClassLoader => JobsLoader) extends JobInfoExtractor {

  override def extractInfo(file: File, className: String): TryLoad[JobInfo] = {
    val prev = Thread.currentThread().getContextClassLoader
    try {
      val existing = this.getClass.getClassLoader
      val url = file.toURI.toURL
      val clzLoader = new URLClassLoader(Array(url), existing)
      Thread.currentThread().setContextClassLoader(clzLoader)

      val loader = mkLoader(clzLoader)
      val executeJobInstance = loader.loadJobInstance(className, Action.Execute)
      executeJobInstance orElse loader.loadJobInstance(className, Action.Serve) map { instance =>
        val lang = instance match {
          case _: JavaJobInstance => JobInfoData.JavaLang
          case _ => JobInfoData.ScalaLang
        }
        JobInfo(instance, ExtractedData(
          lang = lang,
          execute = instance.describe().collect { case x: UserInputArgument => x },
          isServe = !executeJobInstance.isSuccess,
          tags = instance.describe()
            .collect { case InternalArgument(t) => t }
            .flatten
        ))
      }
    } catch {
      // wrap error with better explanation +
      // transporting it over Future leads to unclear error
      // see scala.concurrent.impl.Promise.resolver
      case e: LinkageError =>
        val msg = s"Handled LinkageError: ${e.getClass} ${e.getMessage} (check your artifact or mist library version compatibility)"
        val err = new IllegalStateException(msg)
        Err(err)
      case e: Throwable => Err(e)
    } finally {
      Thread.currentThread().setContextClassLoader(prev)
    }
  }

}

object JvmJobInfoExtractor {

  def apply(): JvmJobInfoExtractor = new JvmJobInfoExtractor(clzLoader => new JobsLoader(clzLoader))

}

class PythonJobInfoExtractor extends JobInfoExtractor {
  override def extractInfo(file: File, className: String) = Succ(
    JobInfo(data = ExtractedData(
      lang = JobInfoData.PythonLang
    )))
}

class BaseJobInfoExtractor(
  jvmJobInfoExtractor: JvmJobInfoExtractor,
  pythonJobInfoExtractor: PythonJobInfoExtractor
) extends JobInfoExtractor {

  override def extractInfo(file: File, className: String): TryLoad[JobInfo] =
    selectExtractor(file)
      .extractInfo(file, className)

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