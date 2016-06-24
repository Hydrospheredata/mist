package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.{Job, JobConfiguration}

private[mist] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: String)

  case class WorkerDidStart(name: String, address: String)

  case class AddJobToRecovery(jobId: String, jobConfiguration: JobConfiguration)

  case class RemoveJobFromRecovery(jobId: String)
}


