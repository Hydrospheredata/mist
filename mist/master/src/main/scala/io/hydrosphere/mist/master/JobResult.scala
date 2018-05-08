package io.hydrosphere.mist.master

import mist.api.data.{JsData, JsUnit}

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  */
case class JobResult(
  success: Boolean,
  payload: JsData,
  errors: List[String])

object JobResult {

  def success(payload: JsData): JobResult = {
    JobResult(
      success = true,
      payload = payload,
      errors = List.empty)
  }

  def failure(errors: List[String]): JobResult = {
    JobResult(
      success = false,
      payload = JsUnit,
      errors = errors)
  }

  def failure(error: String): JobResult =
    failure(List(error))
}
