package io.hydrosphere.mist.worker

private[mist] class JobDescription(val UID: () => String,
                                   val Time: String,
                                   val namespace: String,
                                   val externalId: Option[String] = None,
                                   val router: Option[String] = None)
