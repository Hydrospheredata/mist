package io.hydrosphere.mist.job

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.{ExtractedFunctionData, FunctionInfoData, FunctionInstanceLoader}
import io.hydrosphere.mist.python.{FunctionInfoPythonExecuter, PythonCmd}
import io.hydrosphere.mist.utils.{Err, Logger, Succ, TryLoad}
import mist.api.internal.{BaseFunctionInstance, FunctionInstance, JavaFunctionInstance, PythonFunctionInstance}
import io.hydrosphere.mist.utils.{Err, Succ, TryLoad}
import mist.api.{ArgInfo, InternalArgument, UserInputArgument}
import org.apache.commons.io.FilenameUtils

case class FunctionInfo(
  instance: BaseFunctionInstance = FunctionInstance.NoOpInstance,
  data: ExtractedFunctionData
)

trait FunctionInfoExtractor {
  def extractInfo(file: File, className: String): TryLoad[FunctionInfo]
}

class JvmFunctionInfoExtractor(mkLoader: ClassLoader => FunctionInstanceLoader) extends FunctionInfoExtractor {

  override def extractInfo(file: File, className: String): TryLoad[FunctionInfo] = {
    val prev = Thread.currentThread().getContextClassLoader
    try {
      val existing = this.getClass.getClassLoader
      val url = file.toURI.toURL
      val clzLoader = new URLClassLoader(Array(url), existing)
      Thread.currentThread().setContextClassLoader(clzLoader)

      val loader = mkLoader(clzLoader)
      val executeFnInstance = loader.loadFnInstance(className, Action.Execute)
      executeFnInstance orElse loader.loadFnInstance(className, Action.Serve) map { instance =>
        val lang = instance match {
          case _: JavaFunctionInstance => FunctionInfoData.JavaLang
          case _ => FunctionInfoData.ScalaLang
        }
        FunctionInfo(instance, ExtractedFunctionData(
          lang = lang,
          execute = instance.describe().collect { case x: UserInputArgument => x },
          isServe = !executeFnInstance.isSuccess,
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

class PythonFunctionInfoExtractor(mkExecuter: (File, String) => PythonCmd[Seq[ArgInfo]]) extends FunctionInfoExtractor with Logger {

  override def extractInfo(file: File, className: String): TryLoad[FunctionInfo] = {
    val executer = mkExecuter(file, className)
    //TODO: define context value
    val args = executer.invoke() match {
      case Left(ex) =>
        logger.error(ex.getLocalizedMessage, ex)
        Seq.empty
      case Right(out) => out
    }

    val instance = new PythonFunctionInstance(args)
    Succ(FunctionInfo(
      instance,
      ExtractedFunctionData(
        className,
        FunctionInfoData.PythonLang,
        args.collect { case u: UserInputArgument => u },
        isServe = false,
        args.collect { case InternalArgument(t) => t }.flatten
      )
    ))
  }
}

object PythonFunctionInfoExtractor {

  def apply(): PythonFunctionInfoExtractor = {
    val mkExecuter = (file: File, fnName: String) => new FunctionInfoPythonExecuter(file, fnName)
    new PythonFunctionInfoExtractor(mkExecuter)
  }
}

object JvmFunctionInfoExtractor {

  def apply(): JvmFunctionInfoExtractor = new JvmFunctionInfoExtractor(clzLoader => new FunctionInstanceLoader(clzLoader))

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
      case "py" | "egg" => pythonJobInfoExtractor
      case "jar" => jvmJobInfoExtractor
    }
}

object FunctionInfoExtractor {
  def apply(): BaseFunctionInfoExtractor =
    new BaseFunctionInfoExtractor(JvmFunctionInfoExtractor(), PythonFunctionInfoExtractor())
}