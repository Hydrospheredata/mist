package io.hydrosphere.mist.master

import mist.api.data.{JsLikeData, JsLikeUnit}

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  */
case class JobResult(
  success: Boolean,
  payload: JsLikeData,
  errors: List[String])

object JobResult {

  def success(payload: JsLikeData): JobResult = {
    JobResult(
      success = true,
      payload = payload,
      errors = List.empty)
  }

  def failure(errors: List[String]): JobResult = {
    JobResult(
      success = false,
      payload = JsLikeUnit,
      errors = errors)
  }

  def failure(error: String): JobResult =
    failure(List(error))
}
