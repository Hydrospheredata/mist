package io.hydrosphere.mist.master.namespace

import java.io.File

import akka.actor._
import cats.implicits._
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.runners.jar.JobsLoader
import io.hydrosphere.mist.master.namespace.JobMessages._
import io.hydrosphere.mist.utils.Logger

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  promise: Future[Either[String, Map[String, Any]]]
)

trait JobRunner {

  def run(params: JobParams, context: NamedContext): Either[String, Map[String, Any]]

}

object JobRunner {

  //TODO: only jar
  val ScalaRunner = new JobRunner with Logger {
    override def run(
      params: JobParams,
      context: NamedContext): Either[String, Map[String, Any]] = {
      import params._

      val file = new File(filePath)
      if (!file.exists()) {
        Left(s"Can not found file: $filePath")
      } else {
        try {
          context.addJar(params.filePath)
          val load = JobsLoader.fromJar(file).loadJobInstance(className, action)
          Either.fromTry(load).flatMap(instance => {
            instance.run(context.setupConfiguration, arguments)
          }).leftMap(e => buildErrorMessage(params, e))
        } catch {
          case e: Throwable =>
            logger.error("WTD?", e)
            Left(e.getMessage)
        }
      }
    }

    private def buildErrorMessage(params: JobParams, e: Throwable): String = {
      val msg = Option(e.getMessage).getOrElse("")
      val line = e.getStackTrace.headOption.map(e => e.toString).getOrElse("")
      s"Error running job with $params. Type: ${e.getClass.getCanonicalName}, message: $msg, trace head $line"
    }
  }

}

class WorkerActor(
  name: String,
  namedContext: NamedContext,
  runner: JobRunner,
  idleTimeout: Duration,
  maxJobs: Int
) extends Actor with ActorLogging {

  import java.util.concurrent.Executors.newFixedThreadPool

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  implicit val jobsContext = ExecutionContext.fromExecutorService(newFixedThreadPool(maxJobs))

  override def receive: Receive = {
    case req @ RunJobRequest(id, params) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        val future = startJob(req)
        activeJobs += id -> ExecutionUnit(sender(), future)
        sender() ! JobStarted(id)
      }

    // TODO: test
    case CancelJobRequest(id) =>
      log.info(s"TRY CANCEL JOB $id")
      activeJobs.get(id) match {
        case Some(u) =>
          log.info(s"TRY CANCEL JOB 2 $id")
          namedContext.context.cancelJobGroup(id)
          log.info(s"TRY CANCEL JOB 3 $id")
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
            log.info(s"ERRROROR $error")
            self ! JobFailure(id, error)
          case Right(value) => self ! JobSuccess(id, value)
        }

      case Failure(e) =>
        log.error(e, "WTF??")
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
    Props(classOf[WorkerActor], name, context, JobRunner.ScalaRunner, idleTimeout, 10)

}
