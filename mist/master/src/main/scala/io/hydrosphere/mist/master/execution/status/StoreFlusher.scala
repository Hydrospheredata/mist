package io.hydrosphere.mist.master.execution.status

import akka.actor._
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.logging.{JobLogger, LogService}
import io.hydrosphere.mist.master.store.JobRepository

import scala.concurrent.Future

/**
  * Root entry for flushing job statuses
  */
class StoreFlusher(
  get: String => Future[JobDetails],
  update: JobDetails => Future[Unit],
  jobLoggerF: String => JobLogger
) extends Actor {

  import StoreFlusher._

  override def receive: Receive = process(Map.empty)

  private def process(flushers: Map[String, ActorRef]): Receive = {
    case ev: ReportedEvent =>
      val id = ev.e.id
      val ref = flushers.get(id) match {
        case Some(r) => r
        case None =>
          val props = JobStatusFlusher.props(id, get, update, jobLoggerF)
          val ref = context.actorOf(props)
          context watchWith(ref, Died(id))
      }
      ref forward ev
      context become process(flushers + (id -> ref))

    case Died(id) =>
      context become process(flushers - id)
  }

}

object StoreFlusher {

  case class Died(id: String)

  def props(
    get: String => Future[JobDetails],
    update: JobDetails => Future[Unit],
    jobLoggerF: String => JobLogger
  ): Props = Props(new StoreFlusher(get, update, jobLoggerF))

  def props(repo: JobRepository, logService: LogService): Props = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val get = (id: String) => {
      repo.get(id).flatMap({
        case Some(d) => Future.successful(d)
        case None => Future.failed(new IllegalStateException(s"Couldn't find job with id: $id"))
      })
    }
    props(
      get = get,
      update = repo.update,
      logService.getJobLogger
    )
  }

}

