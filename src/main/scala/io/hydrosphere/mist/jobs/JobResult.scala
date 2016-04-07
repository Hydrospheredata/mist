package io.hydrosphere.mist.jobs

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  * @param request user request
  */
private[mist] case class JobResult(success: Boolean, payload: Map[String, Any], errors: List[String], request: JobConfiguration)
