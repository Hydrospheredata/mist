package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorRef, Props}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.JobQueue.{DequeueJob, EnqueueJob}
import io.hydrosphere.mist.utils.Logger
import org.joda.time.DateTime

import scala.language.postfixOps

object JobDispatcher {
  
  def props(): Props = Props(classOf[JobDispatcher])
  
}

class JobDispatcher extends Actor with Logger {

  private val jobQueueActor: ActorRef = context.actorOf(JobQueue.props())


  override def preStart(): Unit = {
    super.preStart()
    logger.debug("JobDispatcher: starting")
  }

  override def receive: Receive = {
    case jobDetails: JobDetails =>
      logger.debug(s"Received new JobDetails: $jobDetails")
      
      // Add job into queue
      jobQueueActor ! EnqueueJob(jobDetails)

      // Add job into history
      JobRepository().update(jobDetails)
      
      context become jobStarted(sender)
  }
  
  def jobStarted(originalSender: ActorRef): Receive = {
    case job: JobDetails => 
      logger.debug(s"Job was started at ${new DateTime(job.startTime.getOrElse(0L)).toString}")
      // Update job in history store
      JobRepository().update(job)
      context become getResult(originalSender)
  }
  
  def getResult(originalSender: ActorRef): Receive = {
    case job: JobDetails =>
      val time = job.endTime.getOrElse(0L) - job.startTime.getOrElse(0L)
      logger.debug(s"Job finished at ${new DateTime(job.endTime.getOrElse(0L)).toString} (${time}ms): $job)")
      // Remove job from queue
      jobQueueActor ! DequeueJob(job)

      // Update job in history store
      JobRepository().update(job)
    
      originalSender ! job
  }

  override def postStop(): Unit = {
    super.postStop()
    logger.debug("JobDispatcher: stopping")
  }
}
