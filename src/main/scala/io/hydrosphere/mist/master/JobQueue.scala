package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.JobQueue.DequeueJob
import io.hydrosphere.mist.utils.Logger

object JobQueue {
 
  case class EnqueueJob(jobDetails: JobDetails)
  case class DequeueJob(jobDetails: JobDetails)

  def props()(implicit parentContext: ActorRefFactory): Props = 
    Props(classOf[JobQueue], parentContext.actorOf(JobManager.props()), JobRepository())
  
}

class JobQueue(jobManager: ActorRef, store: JobRepository) extends Actor with Logger {

  import io.hydrosphere.mist.master.JobQueue.EnqueueJob


  override def preStart(): Unit = {
    super.preStart()
    logger.debug("JobQueue: starting")
  }

  override def receive: Receive = {
    case EnqueueJob(job) =>
      val queuedJob = job.withStatus(JobDetails.Status.Queued)
      store.update(queuedJob)
      logger.debug(s"Adding job to `${queuedJob.configuration.namespace}` queue")
      startJobs(queuedJob.configuration.namespace)
      context become dequeueJob(sender)
  }

  def dequeueJob(originalSender: ActorRef): Receive = {
    case DequeueJob(jobDetails) =>
      logger.debug(s"Received result")
      startJobs(jobDetails.configuration.namespace)
    case job: JobDetails =>
      // just pass it further
      originalSender ! job
  }
  
  private def startJobs(namespace: String): Unit = {
    var namespaceQueue = store.queuedInNamespace(namespace)
    var runningJobs = store.runningInNamespace(namespace)
    while (runningJobs.length < MistConfig.Contexts.maxParallelJobs(namespace)) {
      if (namespaceQueue.nonEmpty) {
        val queuedJob = namespaceQueue.head
        namespaceQueue = namespaceQueue diff List(queuedJob)
        val job = queuedJob.withStatus(JobDetails.Status.Running)
        logger.debug(s"Starting job in $namespace. ${runningJobs.length} jobs are started")
        store.update(job)
        jobManager ! JobManager.StartJob(job)
        runningJobs = runningJobs :+ job
      } else {
        return
      }
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    logger.debug("JobQueue: stopping")
  }
}
