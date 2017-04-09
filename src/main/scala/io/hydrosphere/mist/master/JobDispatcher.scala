package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.JobQueue.{DequeueJob, EnqueueJob}
import io.hydrosphere.mist.utils.Logger
import org.joda.time.DateTime

import scala.language.postfixOps

object JobDispatcher {
  
  def props()(implicit parentContext: ActorRefFactory): Props = 
    Props(classOf[JobDispatcher], parentContext.actorOf(JobQueue.props()), JobRepository())
  
}

class JobDispatcher(jobQueue: ActorRef, store: JobRepository) extends Actor with Logger {

  private implicit val parentContext = context

  override def preStart(): Unit = {
    super.preStart()
    logger.debug("JobDispatcher: starting")
  }

  override def receive: Receive = {
    case jobDetails: JobDetails =>
      logger.debug(s"Received new JobDetails: $jobDetails")
      
      // Add job into queue
      jobQueue ! EnqueueJob(jobDetails)

      // Add job into history
      store.update(jobDetails)
      
      context become jobStarted(sender)
  }
  
  def jobStarted(originalSender: ActorRef): Receive = {
    case job: JobDetails if job.status.isFinished =>
      val time = job.endTime.getOrElse(0L) - job.startTime.getOrElse(0L)
      logger.debug(s"Job finished at ${new DateTime(job.endTime.getOrElse(0L)).toString} (${time}ms): $job)")
      // Remove job from queue
      jobQueue ! DequeueJob(job)

      // Update job in history store
      store.update(job)

      originalSender ! job

    case job: JobDetails =>
      logger.debug(s"Job was started at ${new DateTime(job.startTime.getOrElse(0L)).toString}")
      // Update job in history store
      store.update(job)
  }

  override def postStop(): Unit = {
    super.postStop()
    logger.debug("JobDispatcher: stopping")
  }
}
