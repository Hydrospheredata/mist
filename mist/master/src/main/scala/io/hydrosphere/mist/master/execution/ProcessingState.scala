package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.JobResult
import io.hydrosphere.mist.master.models.ContextConfig

import scala.collection.immutable.Queue
import scala.concurrent.Promise

class ProcessingState(jobs: Map[String, JobStatus], queue: Queue[String]) {

  private def update(
    m: Map[String, JobStatus] = jobs,
    q: Queue[String] = queue
  ): ProcessingState = new ProcessingState(m, q)

  def queued: Seq[JobStatus] = jobs.values.filter(_.state == ExecState.Queued).toSeq

  def enqueue(req: RunJobRequest): (ProcessingState, JobStatus) = {
    val id = req.id
    val entry = JobStatus(req, ExecState.Queued, Promise[JobResult])
    val next = update(
      jobs + (id -> entry),
      queue.enqueue(id)
    )
    (next, entry)
  }

  def remove(id: String): (ProcessingState, JobStatus) = {
    val entry = jobs(id)
    val next = update(
      jobs - id,
      queue.filter(_ != id)
    )
    (next, entry)
  }

  def get(id: String): Option[JobStatus] = jobs.get(id)

  def changeState(id: String, state: ExecState): ProcessingState = {
    val entry = jobs(id).copy(state = state)
    update(jobs + (id -> entry))
  }

  def markStarted(id: String): ProcessingState = changeState(id, ExecState.Started)

  def next: Option[JobStatus] = if (queue.isEmpty) None else Option(jobs(queue.front))

  def hasQueued: Boolean = queue.isEmpty

  def isEmpty: Boolean = jobs.isEmpty

  def nonEmpty: Boolean = !isEmpty

}
