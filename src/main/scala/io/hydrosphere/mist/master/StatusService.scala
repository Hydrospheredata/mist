package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorLogging, Props}
import akka.actor.Actor.Receive
import io.hydrosphere.mist.Messages.JobMessages.JobStarted
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.Messages.WorkerMessages.GetActiveJobs
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.store.JobRepository

class StatusService(store: JobRepository) extends Actor with ActorLogging {

  val activeStatuses = List(Status.Queued, Status.Running, Status.Initialized)

  override def receive: Receive = {
    case JobStarted(id, time) =>
     actualDetails(id).foreach(d => {
       val updated = d.withStartTime(time)
       store.update(updated)
     })

    case Register(id, params, source) =>
      val details = JobDetails(params, source, id)
      store.update(details)

    case x:UpdateStatus => handleUpdateStatus(x)

    case RunningJobs =>
      sender() ! store.filteredByStatuses(activeStatuses)
  }

  private def handleUpdateStatus(u: UpdateStatus): Unit = {
    val details = actualDetails(u.id)
    val updated = details.map(d => {
      val updatedTimes = u.status match {
        case Status.Running => d.withStartTime(u.time)
        case Status.Stopped => d.withEndTime(u.time)
        case _ => d
      }
      updatedTimes.withStatus(u.status)
    })
    updated.foreach(store.update)
  }

  private def actualDetails(id: String): Option[JobDetails] =
    store.get(id)
}

object StatusService {

  def props(store: JobRepository): Props =
    Props(classOf[StatusService], store)
}
