package io.hydrosphere.mist.master

import java.util.Date

import akka.actor.{ActorRef, Actor, ActorSelection, RootActorPath}
import akka.cluster.ClusterEvent.{MemberEvent, MemberExited, MemberUp}
import akka.cluster.{Cluster, Member}
import io.hydrosphere.mist.Messages.StopJob
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.master.JobManager.StartJob
import io.hydrosphere.mist.master.RemoveWorker._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError

import scala.collection.mutable
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Failure, Success}

class Namespace(name: String) {


}


class RemoteWorkerFrontend(name: String) extends Actor {

  val cluster = Cluster(context.system)

  var worker: Option[ActorSelection] = None

  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])

  def noWorker: Receive = {

  }

  def withWorker: Receive = {

  }

  def handleMemberEvent: Receive = {
    case MemberUp(member) if isMyWorker(member) =>
      val selection = cluster.system.actorSelection(RootActorPath(member.address) / "user"/ workerName)
      worker = Some(selection)

    case MemberExited(member) if isMyWorker(member) =>
      worker = None
  }

  private def isMyWorker(member: Member): Boolean =
    member.hasRole(workerName)

  private def workerName: String = s"worker-$name"
}

case class ExecutionUnit(
  requester: ActorRef,
  promise: Future[JobResponseOrError]
)

class RemoteWorker(
  name: String,
  maxJobs: Int = 1
) extends Actor with Logger {

  import java.util.concurrent.Executors.newFixedThreadPool

  val cluster = Cluster(context.system)
  val target = targetAddress()

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  val jobsContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxJobs))

  val sparkContext = NamedContext(name)

  override def receive: Receive = {
    case StartJob(details) =>
      logger.info(s"Starting job: $details")
      val id = details.jobId
      val f = startJob(details)
      activeJobs += id -> ExecutionUnit(sender(), f)
      sender() ! JobStarted(id)

    case StopJob(id) =>
      logger.warn("Stop job id not implemented!")

    case x: JobStatus =>
      logger.info(s"Jon execution done. Result $x")
      activeJobs.get(x.id) match {

        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          logger.warn(s"Corrupted worker state, unexpected receiving $x")
      }
      target ! x
  }

  private def startJob(details: JobDetails): Future[JobResponseOrError] = {
    logger.info(s"Starting job: $details")
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

object RemoveWorker {

  // internal messages
  sealed trait JobStatus {
    val id: String
  }

  case class JobSuccess(id: String, result: Map[String, Any]) extends JobStatus
  case class JobFailure(id: String, error: String) extends JobStatus

  case class JobStarted(id: String) extends JobStatus
}
