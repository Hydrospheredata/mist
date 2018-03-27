package io.hydrosphere.mist.worker.runners.python

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.python.PythonFunctionExecuter
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.runners.JobRunner
import io.hydrosphere.mist.worker.{NamedContext, SparkArtifact}
import mist.api.data.JsLikeData

class PythonRunner(artifact: SparkArtifact) extends JobRunner with Logger {

  override def run(request: RunJobRequest, context: NamedContext): Either[Throwable, JsLikeData] = {
    val newReq = request.copy(params = request.params.copy(filePath = artifact.local.getAbsolutePath))
    //Move to class dependency
    val executer = new PythonFunctionExecuter(newReq, context)
    executer.invoke(Some(context.sparkConf))
  }
}
