package io.hydrosphere.mist.worker

import java.util.concurrent.Executors

import akka.actor._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.WorkerMessages.WorkerInitInfo
import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.worker.runners.{JobRunner, MistJobRunner}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  jobFuture: Future[Either[String, Map[String, Any]]]
)

trait JobStarting { that: Actor =>

  val namedContext: NamedContext
  val runner: JobRunner

  def startJob(req: RunJobRequest)(implicit ec: ExecutionContext): Future[Either[String, Map[String, Any]]] = {
    val id = req.id
    val future = Future {
      namedContext.sparkContext.setJobGroup(req.id, req.id)
      runner.run(req, namedContext)
    }

    future.onComplete(r => {
      val message = r match {
        case Success(Left(error)) => JobFailure(id, error)
        case Success(Right(value)) => JobSuccess(id, value)
        case Failure(e) => JobFailure(id, e.getMessage)
      }
      self ! message
    })

    future
  }
}

class SharedWorkerActor(
  val namedContext: NamedContext,
  val runner: JobRunner,
  idleTimeout: Duration,
  maxJobs: Int
) extends Actor with JobStarting with ActorLogging {

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  implicit val ec = {
    val service = Executors.newFixedThreadPool(maxJobs)
    ExecutionContext.fromExecutorService(
      service,
      e => log.error(e, "Error from thread pool")
    )
  }

  override def preStart(): Unit = {
    context.setReceiveTimeout(idleTimeout)
  }

  override def receive: Receive = {
    case req @ RunJobRequest(id, params) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        val future = startJob(req)
        log.info(s"Starting job: $id")
        activeJobs += id -> ExecutionUnit(sender(), future)
        sender() ! JobStarted(id)
      }

    // it does not work for streaming jobs
    case CancelJobRequest(id) =>
      activeJobs.get(id) match {
        case Some(u) =>
          namedContext.sparkContext.cancelJobGroup(id)
          sender() ! JobIsCancelled(id)
        case None =>
          log.warning(s"Can not cancel unknown job $id")
      }

    case x: JobResponse =>
      log.info(s"Job execution done. Result $x")
      activeJobs.get(x.id) match {
        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          log.warning(s"Corrupted worker state, unexpected receiving {}", x)
      }

    case ReceiveTimeout if activeJobs.isEmpty =>
      log.info(s"There is no activity on worker: $namedContext.. Stopping")
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
    namedContext.stop()
  }

}

object SharedWorkerActor {

  def props(
    context: NamedContext,
    jobRunner: JobRunner,
    idleTimeout: Duration,
    maxJobs: Int
  ): Props =
    Props(classOf[SharedWorkerActor], context, jobRunner, idleTimeout, maxJobs)

}

class ExclusiveWorkerActor(
  val namedContext: NamedContext,
  val runner: JobRunner
) extends Actor with JobStarting with ActorLogging {

  implicit val ec = {
    val service = Executors.newSingleThreadExecutor()
    ExecutionContext.fromExecutorService(service)
  }

  override def receive: Receive = awaitRequest

  val awaitRequest: Receive = {
    case req @ RunJobRequest(id, params) =>
      val future = startJob(req)
      sender() ! JobStarted(id)
      val unit = ExecutionUnit(sender(), future)
      context.become(execute(unit))
      log.info(s"Starting job: $id")
  }

  def execute(executionUnit: ExecutionUnit): Receive = {
    case CancelJobRequest(id) =>
      namedContext.sparkContext.cancelJobGroup(id)
      sender() ! JobIsCancelled(id)
      context.stop(self)

    case x: JobResponse =>
      log.info(s"Job execution done. Result $x")
      executionUnit.requester forward x
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
    namedContext.stop()
  }

}

object ExclusiveWorkerActor {

  def props(context: NamedContext, jobRunner: JobRunner): Props =
    Props(classOf[ExclusiveWorkerActor], context, jobRunner)
}

object WorkerActor {

  def propsFromInitInfo(name: String, contextName: String, mode: WorkerMode): WorkerInitInfo => (NamedContext, Props) = {

    def mkNamedContext(info: WorkerInitInfo): NamedContext = {
      import info._

      val conf = new SparkConf().setAppName(name).setAll(info.sparkConf)
      val sparkContext = new SparkContext(conf)

      val centralLoggingConf = {
        val hostPort = logService.split(":")
        CentralLoggingConf(hostPort(0), hostPort(1).toInt)
      }

      new NamedContext(
        sparkContext,
        contextName,
        org.apache.spark.streaming.Duration(info.streamingDuration.toMillis),
        Option(centralLoggingConf)
      )
    }

    (info: WorkerInitInfo) => {
      val namedContext = mkNamedContext(info)
      val props = mode match {
        case Shared => SharedWorkerActor.props(namedContext, MistJobRunner, info.downtime, info.maxJobs)
        case Exclusive => ExclusiveWorkerActor.props(namedContext, MistJobRunner)
      }
      (namedContext, props)
    }
  }

}
