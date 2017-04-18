package io.hydrosphere.mist.jobs

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
  request: JobExecutionParams)

object JobResult {

  def success(payload: JobResponse, request: JobExecutionParams): JobResult = {
    JobResult(
      success = true,
      payload = payload,
      errors = List.empty,
      request = request)
  }

  def failure(errors: List[String], request: JobExecutionParams): JobResult = {
    JobResult(
      success = false,
      payload = Map.empty,
      errors = errors,
      request = request)
  }

  def failure(error: String, request: JobExecutionParams): JobResult =
    failure(List(error), request)
}
