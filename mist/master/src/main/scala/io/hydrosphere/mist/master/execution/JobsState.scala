package io.hydrosphere.mist.master.execution

import akka.actor.ActorRef

import scala.collection.immutable.Queue

class JobsState(actors: Map[String, ActorRef], queue: Queue[String]) {

  def enqueue(id: String, ref: ActorRef): JobsState =
    JobsState(actors + (id -> ref), queue.enqueue(id))

  def head: Option[ActorRef] =
    headId.flatMap(id => actors.get(id))

  def headId: Option[String] = if (queue.isEmpty) None else Some(queue.front)

  def get(id: String): Option[ActorRef] = actors.get(id)

  def hasQueued(id: String): Boolean = queue.contains(id)

  def has(id: String): Boolean = get(id).isDefined

  def remove(id: String): JobsState =
    JobsState(actors - id, queue.filter(_ != id))

  def removeFromQueue(id: String): JobsState =
    JobsState(actors, queue.filter(_ != id))

  def take(n: Int): Map[String, ActorRef] = {
    val min = math.min(n, queue.size)
    queue.take(min).map(i => i -> actors(i)).toMap
  }
}

object JobsState {

  val empty: JobsState = JobsState(Map.empty[String, ActorRef], Queue.empty)

  def apply(actors: Map[String, ActorRef], queue: Queue[String]): JobsState = new JobsState(actors, queue)

  def apply(id: String, ref: ActorRef): JobsState = empty.enqueue(id, ref)
}

