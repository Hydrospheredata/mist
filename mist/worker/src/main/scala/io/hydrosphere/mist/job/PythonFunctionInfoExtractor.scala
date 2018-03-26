package io.hydrosphere.mist.job

import java.io.File

import io.hydrosphere.mist.core.jvmjob.{ExtractedFunctionData, FunctionInfoData}
import io.hydrosphere.mist.python.FunctionInfoPythonExecuter
import io.hydrosphere.mist.utils.{Logger, Succ, TryLoad}
import mist.api.args._
import mist.api.internal.PythonFunctionInstance

class PythonFunctionInfoExtractor extends FunctionInfoExtractor with Logger {

  override def extractInfo(file: File, className: String): TryLoad[FunctionInfo] = {
    val executer = new FunctionInfoPythonExecuter(file, className)

    //TODO: define context value
    val args = executer.invoke("get_metadata") match {
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
