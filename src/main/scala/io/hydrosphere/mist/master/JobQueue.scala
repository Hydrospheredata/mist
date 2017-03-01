package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorRef, Props}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.master.JobQueue.DequeueJob
import io.hydrosphere.mist.utils.Logger

import scala.collection.mutable

object JobQueue {
 
  case class EnqueueJob(jobDetails: JobDetails)
  case class DequeueJob(jobDetails: JobDetails)

  private val queue: mutable.Map[String, mutable.Queue[JobDetails]] = mutable.Map[String, mutable.Queue[JobDetails]]()
  private val running: mutable.Map[String, mutable.ArrayBuffer[JobDetails]] = mutable.Map[String, mutable.ArrayBuffer[JobDetails]]()

  def props(): Props = Props(classOf[JobQueue])
  
}

class JobQueue extends Actor with Logger {

  import io.hydrosphere.mist.master.JobQueue.EnqueueJob


  override def preStart(): Unit = {
    super.preStart()
    logger.debug("JobQueue: starting")
  }

  override def receive: Receive = {
    case EnqueueJob(job) =>
      val namespaceQueue = JobQueue.queue.getOrElseUpdate(job.configuration.namespace, mutable.Queue[JobDetails]())
      namespaceQueue += job
      logger.debug(s"Adding job to `${job.configuration.namespace}` queue (current queue size: ${namespaceQueue.length})")
      startJobs(job.configuration.namespace)
      context become dequeueJob(sender)
  }

  def dequeueJob(originalSender: ActorRef): Receive = {
    case DequeueJob(jobDetails) =>
      val runningJobs = JobQueue.running.getOrElse(jobDetails.configuration.namespace, mutable.ArrayBuffer[JobDetails]())
      runningJobs -= jobDetails
      logger.debug(s"Received result; ${runningJobs.length} jobs left")
      startJobs(jobDetails.configuration.namespace)
    case job: JobDetails =>
      // just pass it further
      originalSender ! job
  }
  
  private def startJobs(namespace: String): Unit = {
    val namespaceQueue = JobQueue.queue.getOrElse(namespace, mutable.Queue[JobDetails]())
    val runningJobs = JobQueue.running.getOrElse(namespace, mutable.ArrayBuffer[JobDetails]())
    while (runningJobs.length < MistConfig.Contexts.maxParallelJobs(namespace)) {
      if (namespaceQueue.nonEmpty) {
        val job = namespaceQueue.dequeue()
        logger.debug(s"Starting job in $namespace. ${runningJobs.length} jobs are started")
        runningJobs += job
        context.actorOf(ClusterManager.props()) ! ClusterManager.StartJob(job)
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
