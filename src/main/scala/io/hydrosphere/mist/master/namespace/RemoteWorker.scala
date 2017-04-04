package io.hydrosphere.mist.master.namespace

import akka.actor._
import akka.cluster._
import io.hydrosphere.mist.Messages.StopJob
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.master.JobManager._
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError

import scala.collection.mutable
import scala.concurrent._

import scala.concurrent.ExecutionContext.Implicits.global

import RemoteWorker._

import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  promise: Future[JobResponseOrError]
)

class RemoteWorker(
  name: String,
  maxJobs: Int
) extends Actor with ActorLogging {

  import java.util.concurrent.Executors.newFixedThreadPool

  val cluster = Cluster(context.system)
  val target = targetAddress()

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  val jobsContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxJobs))

  val sparkContext = NamedContext(name)

  override def receive: Receive = {
    case StartJob(details) =>
      log.info(s"Starting job: $details")
      val id = details.jobId
      val f = startJob(details)
      activeJobs += id -> ExecutionUnit(sender(), f)
      sender() ! JobStarted(id)

    case StopJob(id) =>
      log.warning("Stop job id not implemented!")

    case x: JobResponse =>
      log.info(s"Jon execution done. Result $x")
      activeJobs.get(x.id) match {

        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          log.warning(s"Corrupted worker state, unexpected receiving {}", x)
      }
  }

  private def startJob(details: JobDetails): Future[JobResponseOrError] = {
    log.info(s"Starting job: $details")
    val id = details.jobId

    val future = Future({
      val runner = Runner(details, sparkContext)
      runner.run()
    })(jobsContext)

    future.onComplete({
      case Success(result) =>
        result match {
          case Right(error) => self ! JobFailure(id, error)
          case Left(value) => self ! JobSuccess(id, value)
        }

      case Failure(e) =>
        self ! JobFailure(id, e.getMessage)
    })

    future
  }

  private def targetAddress(): ActorSelection = {
    val address = MistConfig.Akka.Worker.serverList.head
    cluster.system.actorSelection(s"$address/user/namespace-$name" )
  }
}
object RemoteWorker {

  def props(name: String): Props =
    Props(classOf[RemoteWorker], name, 10)

  // internal messages
  sealed trait JobResponse {
    val id: String
  }

  case class JobSuccess(id: String, result: Map[String, Any]) extends JobResponse
  case class JobFailure(id: String, error: String) extends JobResponse

  case class JobStarted(id: String) extends JobResponse
}
