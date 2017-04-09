package io.hydrosphere.mist.master

import akka.actor.{Actor, Props}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.JobRecovery.StartRecovery
import io.hydrosphere.mist.master.async.AsyncInterface
import io.hydrosphere.mist.utils.Logger

private[mist] object JobRecovery {
  
  sealed trait Message
  case object StartRecovery extends Message
  
  def props(): Props = Props(classOf[JobRecovery], JobRepository())
  
}

private[mist] class JobRecovery(store: JobRepository) extends Actor with Logger {

  override def receive: Receive = {
    case StartRecovery =>
      store
        .filteredByStatuses(List(JobDetails.Status.Running, JobDetails.Status.Queued, JobDetails.Status.Initialized))
        .groupBy(_.source)
        .foreach({
          case (source: JobDetails.Source, jobs: List[JobDetails]) => source match {
            case s: JobDetails.Source.Async =>
              if (MistConfig.AsyncInterfaceConfig(s.provider).isOn) {
                logger.info(s"${jobs.length} jobs must be sent to ${s.provider}")
                jobs.foreach {
                  job => AsyncInterface.subscriber(s.provider, Some(context)) ! job
                }
              } else {
                logger.debug(s"${jobs.length} jobs must be marked as aborted (cause: ${s.provider} is off in config)")
                jobs.foreach {
                  job => store.update(job.withStatus(JobDetails.Status.Aborted))
                }
              }
            case s: JobDetails.Source =>
              logger.debug(s"${jobs.length} jobs must be marked as aborted (cause: $s is not async)")
              jobs.foreach {
                job => store.update(job.withStatus(JobDetails.Status.Aborted))
              }
          }
        })
  }
  
}
