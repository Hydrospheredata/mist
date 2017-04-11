package io.hydrosphere.mist.worker

import akka.actor._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.worker.runners.{MistJobRunner, JobRunner}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  promise: Future[Either[String, Map[String, Any]]]
)


class WorkerActor(
  name: String,
  namedContext: NamedContext,
  runner: JobRunner,
  idleTimeout: Duration,
  maxJobs: Int
) extends Actor with ActorLogging {

  import java.util.concurrent.Executors.newFixedThreadPool

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  implicit val jobsContext = {
    ExecutionContext.fromExecutorService(
      newFixedThreadPool(maxJobs), (e) => log.error(e, "Error from thread pool")
    )
  }

  override def receive: Receive = {
    case req @ RunJobRequest(id, params) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        val future = startJob(req)
        activeJobs += id -> ExecutionUnit(sender(), future)
        sender() ! JobStarted(id)
      }

    // it does not work for streaming jobs
    case CancelJobRequest(id) =>
      activeJobs.get(id) match {
        case Some(u) =>
          namedContext.context.cancelJobGroup(id)
          sender() ! JobIsCancelled(id)
        case None =>
          log.warning(s"Can not cancel unknown job $id")
      }

    case x: JobResponse =>
      log.info(s"Jon execution done. Result $x")
      activeJobs.get(x.id) match {
        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          log.warning(s"Corrupted worker state, unexpected receiving {}", x)
      }

    case ReceiveTimeout if activeJobs.isEmpty =>
      log.info(s"There is no activity on worker: $name. Stopping")
      context.stop(self)
  }

  private def startJob(req: RunJobRequest): Future[Either[String, Map[String, Any]]] = {
    val id = req.id
    log.info(s"Starting job: $id")

    val future = Future {
      namedContext.context.setJobGroup(req.id, req.id)
      runner.run(req.params, namedContext)
    }

    future.onComplete({
      case Success(result) =>
        result match {
          case Left(error) =>
            self ! JobFailure(id, error)
          case Right(value) =>
            self ! JobSuccess(id, value)
        }

      case Failure(e) =>
        self ! JobFailure(id, e.getMessage)
    })

    future
  }

  override def preStart(): Unit = {
    context.setReceiveTimeout(idleTimeout)
  }

  override def postStop(): Unit = {
    namedContext.stop()
    jobsContext.shutdown()
  }

}
object WorkerActor {

  def props(
    name: String,
    context: NamedContext,
    idleTimeout: Duration,
    maxJobs: Int
  ): Props =
    Props(classOf[WorkerActor], name, context, MistJobRunner, idleTimeout, 10)

  def props(
    name: String,
    context: NamedContext,
    maxJobs: Int
  ): Props =
    props(name, context, Duration.Inf, maxJobs)
}
