package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.{Job}

private[mist] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: String)

  case class WorkerDidStart(name: String, address: String)

  case class AddJobToRecovery(job: Job)

  case class RemoveJobFromRecovery(job: Job)
}
