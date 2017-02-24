package io.hydrosphere.mist.master

import akka.actor.{Actor, Props}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.JobRecovery.StartRecovery
import io.hydrosphere.mist.master.async.AsyncInterface
import io.hydrosphere.mist.utils.Logger

private[mist] object JobRecovery {
  
  sealed trait Message
  case object StartRecovery extends Message
  
  def props(): Props = Props(classOf[JobRecovery])
  
}

private[mist] class JobRecovery extends Actor with Logger {

  override def receive: Receive = {
    case StartRecovery =>
      JobRepository()
        .filteredByStatuses(List(JobDetails.Status.Running, JobDetails.Status.Queued, JobDetails.Status.Initialized))
        .groupBy(_.source)
        .foreach({
          case (source: JobDetails.Source, jobs: List[JobDetails]) => source match {
            case s: JobDetails.Source.Async => 
              logger.info(s"${jobs.length} jobs must be sent to ${s.provider}")
              jobs.foreach {
                job => AsyncInterface.subscriber(s.provider, context.system) ! job
              }
            case _ => logger.debug(s"${jobs.length} jobs must be marked as aborted")
          }
          // TODO: return response 
        })
  }
  
}
