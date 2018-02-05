package io.hydrosphere.mist.job

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.{ExtractedFunctionData, FunctionInfoData, JobsLoader}
import io.hydrosphere.mist.utils.{Err, Succ, TryLoad}
import mist.api.args.{InternalArgument, UserInputArgument}
import mist.api.internal.{BaseJobInstance, JavaJobInstance, JobInstance}
import org.apache.commons.io.FilenameUtils

case class FunctionInfo(
  instance: BaseJobInstance = JobInstance.NoOpInstance,
  data: ExtractedFunctionData
)

trait FunctionInfoExtractor {
  def extractInfo(file: File, className: String): TryLoad[FunctionInfo]
}

class JvmFunctionInfoExtractor(mkLoader: ClassLoader => JobsLoader) extends FunctionInfoExtractor {

  override def extractInfo(file: File, className: String): TryLoad[FunctionInfo] = {
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
          case _: JavaJobInstance => FunctionInfoData.JavaLang
          case _ => FunctionInfoData.ScalaLang
        }
        FunctionInfo(instance, ExtractedFunctionData(
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

object JvmFunctionInfoExtractor {

  def apply(): JvmFunctionInfoExtractor = new JvmFunctionInfoExtractor(clzLoader => new JobsLoader(clzLoader))

}

class PythonFunctionInfoExtractor extends FunctionInfoExtractor {
  override def extractInfo(file: File, className: String) = Succ(
    FunctionInfo(data = ExtractedFunctionData(
      lang = FunctionInfoData.PythonLang
    )))
}

class BaseFunctionInfoExtractor(
  jvmJobInfoExtractor: JvmFunctionInfoExtractor,
  pythonJobInfoExtractor: PythonFunctionInfoExtractor
) extends FunctionInfoExtractor {

  override def extractInfo(file: File, className: String): TryLoad[FunctionInfo] =
    selectExtractor(file)
      .extractInfo(file, className)

  private def selectExtractor(file: File): FunctionInfoExtractor =
    FilenameUtils.getExtension(file.getAbsolutePath) match {
      case "py" => pythonJobInfoExtractor
      case "jar" => jvmJobInfoExtractor
    }
}

object FunctionInfoExtractor {
  def apply(): BaseFunctionInfoExtractor =
    new BaseFunctionInfoExtractor(JvmFunctionInfoExtractor(), new PythonFunctionInfoExtractor)
}