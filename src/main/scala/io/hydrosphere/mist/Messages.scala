package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.FullJobConfiguration

private[mist] object Messages {
  
  sealed trait RemovingMessage {
    val name: String
  }

  case class CreateContext(namespace: String)

  case class StopAllContexts()

  case class RemoveContext(name: String) extends RemovingMessage

  case class WorkerDidStart(namespace: String, address: String)

  case class AddJobToRecovery(jobId: String, jobConfiguration: FullJobConfiguration)

  case class RemoveJobFromRecovery(jobId: String)

  sealed trait AdminMessage
  
  case class StopJob(jobIdentifier: String) extends AdminMessage
 
  case class StopWorker(name: String) extends AdminMessage with RemovingMessage
  
  case class StopAllWorkers() extends AdminMessage

  case class ListWorkers() extends AdminMessage

  case class ListRouters(extended: Boolean = false) extends AdminMessage

  case class ListJobs() extends AdminMessage

}
