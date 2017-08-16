package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.master.models.EndpointStartRequest
import io.hydrosphere.mist.utils.TypeAlias.JobResponse

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  */
case class JobResult(
  success: Boolean,
  payload: JobResponse,
  errors: List[String])

object JobResult {

  def success(payload: JobResponse): JobResult = {
    JobResult(
      success = true,
      payload = payload,
      errors = List.empty)
  }

  def failure(errors: List[String]): JobResult = {
    JobResult(
      success = false,
      payload = Map.empty,
      errors = errors)
  }

  def failure(error: String): JobResult =
    failure(List(error))
}
