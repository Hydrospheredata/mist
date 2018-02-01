package io.hydrosphere.mist.master.execution.status

import akka.actor._
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.store.JobRepository

import scala.concurrent.Future

/**
  * Root entry for flushing job statuses
  */
class StoreFlusher(
  get: String => Future[JobDetails],
  update: JobDetails => Future[Unit]
) extends Actor {

  import StoreFlusher._

  var map = Map.empty[String, ActorRef]

  override def receive: Receive = {
    case ev: UpdateStatusEvent =>
      val ref = map.get(ev.id) match {
        case Some(r) => r
        case None =>
          val props = JobStatusFlusher.props(ev.id, get, update)
          val ref = context.actorOf(props)
          map += ev.id -> ref
          context watchWith(ref, Died(ev.id))
      }
      ref forward ev

    case Died(id) =>
      map -= id
  }

}

object StoreFlusher {

  case class Died(id: String)

  def props(
    get: String => Future[JobDetails],
    update: JobDetails => Future[Unit]
  ): Props = Props(classOf[StoreFlusher], get, update)

  def props(repo: JobRepository): Props = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val get = (id: String) => {
      repo.get(id).flatMap({
        case Some(d) => Future.successful(d)
        case None => Future.failed(new IllegalStateException(s"Couldn't find job with id: $id"))
      })
    }
    props(
      get = get,
      update = repo.update
    )
  }

}

