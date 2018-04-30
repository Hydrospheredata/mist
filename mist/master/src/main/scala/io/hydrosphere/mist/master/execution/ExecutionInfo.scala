package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.JobResult
import io.hydrosphere.mist.master.models.JobStartResponse
import mist.api.data.JsData

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util._


case class ExecutionInfo(
  request: RunJobRequest,
  promise: Promise[JsData]
) {

  def toJobStartResponse: JobStartResponse = JobStartResponse(request.id)

  def toJobResult: Future[JobResult] = {
    val result = Promise[JobResult]
    promise.future.onComplete {
      case Success(r) =>
        result.success(JobResult.success(r))
      case Failure(e) =>
        result.success(JobResult.failure(e.getMessage))
    }
    result.future
  }
}

object ExecutionInfo {

  def apply(req: RunJobRequest): ExecutionInfo =
    ExecutionInfo(req, Promise[JsData])

}

