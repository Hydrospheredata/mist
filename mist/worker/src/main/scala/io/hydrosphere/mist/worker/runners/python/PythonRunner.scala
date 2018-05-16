package io.hydrosphere.mist.worker.runners.python

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.python.PythonFunctionExecutor
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.runners.JobRunner
import io.hydrosphere.mist.worker.{MistScContext, SparkArtifact}
import mist.api.data.JsData

class PythonRunner(artifact: SparkArtifact) extends JobRunner with Logger {

  override def run(request: RunJobRequest, context: MistScContext): Either[Throwable, JsData] = {
    val newReq = request.copy(params = request.params.copy(filePath = artifact.local.getAbsolutePath))
    //Move to class dependency
    val executor = new PythonFunctionExecutor(newReq, context)
    executor.invoke(Some(context.sparkConf))
  }
}
