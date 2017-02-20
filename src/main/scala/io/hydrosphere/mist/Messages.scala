package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.FullJobConfiguration

private[mist] object Messages {
  
  sealed trait RemovingMessage {
    val contextIdentifier: String
  }
  
  sealed trait StopAllMessage

  case class CreateContext(namespace: String)

  case class StopAllContexts() extends StopAllMessage

  case class RemoveContext(contextIdentifier: String) extends RemovingMessage

  case class WorkerDidStart(uid: String, namespace: String, address: String)

  case class AddJobToRecovery(jobId: String, jobConfiguration: FullJobConfiguration)

  case class RemoveJobFromRecovery(jobId: String)

  sealed trait AdminMessage
  
  case class StopJob(jobIdentifier: String) extends AdminMessage
 
  case class StopWorker(contextIdentifier: String) extends AdminMessage with RemovingMessage
  
  case class StopAllWorkers() extends AdminMessage with StopAllMessage

  case class ListWorkers() extends AdminMessage

  case class ListRouters(extended: Boolean = false) extends AdminMessage

  case class ListJobs() extends AdminMessage

  case class StopWhenAllDo() extends AdminMessage

  case class EcsHook(name: String) extends AdminMessage

}
