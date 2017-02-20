package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor.{Actor, ActorRef, Props}
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobDetails}
import io.hydrosphere.mist.master.JobQueue.{DequeueJob, EnqueueJob}
import io.hydrosphere.mist.utils.Logger
import org.joda.time.DateTime

import scala.language.postfixOps

object JobDistributor {
  
  def props(): Props = Props(classOf[JobDistributor])
  
}

class JobDistributor extends Actor with Logger {

  private val jobQueueActor: ActorRef = context.system.actorOf(JobQueue.props())
  
  override def receive: Receive = {
    case message: FullJobConfiguration =>
      logger.debug(s"Received new FullJobConfiguration: $message")
      val jobDetails = JobDetails(message, UUID.randomUUID().toString)
      
      // Add job into queue
      jobQueueActor ! EnqueueJob(jobDetails)

      // Add job into history
      context.system.actorOf(JobHistory.props()) ! JobHistory.AddJob(jobDetails)
      
      context become jobStarted(sender)
  }
  
  def jobStarted(originalSender: ActorRef): Receive = {
    case job: JobDetails => 
      logger.debug(s"Job was started at ${new DateTime(job.startTime.getOrElse(0L)).toString}")
      // Update job in history store
      context.system.actorOf(JobHistory.props()) ! JobHistory.UpdateJob(job)
      context become getResult(originalSender)
  }
  
  def getResult(originalSender: ActorRef): Receive = {
    case job: JobDetails =>
      val time = job.endTime.getOrElse(0L) - job.startTime.getOrElse(0L)
      logger.debug(s"Job finished at ${new DateTime(job.endTime.getOrElse(0L)).toString} (${time}ms): $job)")
      // Remove job from queue
      jobQueueActor ! DequeueJob(job)

      // Update job in history store
      context.system.actorOf(JobHistory.props()) ! JobHistory.UpdateJob(job)
    
      originalSender ! job
  }
}
