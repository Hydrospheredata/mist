package com.provectus.lymph.jobs

private[lymph] case class JobResult(success: Boolean, payload: Map[String, Any], errors: List[String], request: JobConfiguration)
