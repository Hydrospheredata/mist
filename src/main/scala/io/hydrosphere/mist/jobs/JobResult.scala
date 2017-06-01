package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.master.models.JobStartRequest
import io.hydrosphere.mist.utils.TypeAlias.JobResponse

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  * @param request user request
  */
case class JobResult(
  success: Boolean,
  payload: JobResponse,
  errors: List[String],
  request: JobStartRequest)

object JobResult {

  def success(payload: JobResponse, request: JobStartRequest): JobResult = {
    JobResult(
      success = true,
      payload = payload,
      errors = List.empty,
      request = request)
  }

  def failure(errors: List[String], request: JobStartRequest): JobResult = {
    JobResult(
      success = false,
      payload = Map.empty,
      errors = errors,
      request = request)
  }

  def failure(error: String, request: JobStartRequest): JobResult =
    failure(List(error), request)
}
