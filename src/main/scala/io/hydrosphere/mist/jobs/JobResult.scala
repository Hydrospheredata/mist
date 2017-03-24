package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.utils.TypeAlias.JobResponse

/** Used for packing results for response
  *
  * @param success boolean flag of success
  * @param payload job results
  * @param errors possible error list
  * @param request user request
  */
private[mist] case class JobResult(success: Boolean, payload: JobResponse, errors: List[String], request: FullJobConfiguration)
