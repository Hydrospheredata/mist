package io.hydrosphere.mist.master.http

import akka.actor.{PoisonPill, ActorSystem, ActorRef}
import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern._
import akka.util.Timeout
import com.typesafe.config.Config
import io.hydrosphere.mist.Messages.{StopAllWorkers, StopJob, StopWorker}
import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.{JobDispatcher, WorkerLink}
import io.hydrosphere.mist.master.cluster.ClusterManager
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.{JobResponse, JobParameters, JobResponseOrError}

import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

case class JobExecutionStatus(
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized
)

class JobRoutes(config: Config) extends Logger {

  def listDefinition(): Seq[JobDefinition] = {
    JobDefinition.parseConfig(config).flatMap({
      case Success(d) => Some(d)
      case Failure(e) =>
        logger.error("Invalid route configuration", e)
        None
    })
  }

  def listInfos(): Seq[JobInfo] = {
    listDefinition().map(JobInfo.load)
      .flatMap({
        case Success(info) => Some(info)
        case Failure(e) =>
          logger.error("Job's loading failed", e)
          None
      })
  }
}

class MasterService(
  managerRef: ActorRef,
  jobRoutes: JobRoutes,
  system: ActorSystem
) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(1.second)

  private val activeStatuses = List(JobDetails.Status.Running, JobDetails.Status.Queued)

  def jobsStatuses(): Map[String, JobExecutionStatus] = {
    JobRepository().filteredByStatuses(activeStatuses)
      .map(d => d.jobId -> JobExecutionStatus(d.startTime, d.endTime, d.status))
      .toMap
  }

  def workers(): Future[List[WorkerLink]] = {
    val f = managerRef ? ClusterManager.GetWorkers()
    f.mapTo[List[WorkerLink]]
  }

  def stopAllWorkers(): Future[Unit] = {
    val f = managerRef ? StopAllWorkers()
    f.map(_ => ())
  }

  //TODO: if job id unknown??
  def stopJob(id: String): Future[Unit] = {
    val f = managerRef ? StopJob(id)
    f.map(_ => ())
  }

  //TODO: if worker id unknown??
  def stopWorker(id: String): Future[Unit] = {
    val f = managerRef ? StopWorker(id)
    f.map(_ => ())
  }

  def listRoutes(): Seq[JobInfo] = jobRoutes.listInfos()

  //TODO: why we need full configuration ??
  //TODO: for starting job we need only id, action, and params
  def startJob(id: String, action: Action, params: JobParameters): Future[JobResult] = {
    val initial = FullJobConfigurationBuilder().fromRest(id, params)
    val builder = action match {
      case Action.Serve => initial.setServing(true)
      case Action.Train => initial.setTraining(true)
      case Action.Execute => initial
    }
    val configuration = builder.build()
    val distributor = system.actorOf(JobDispatcher.props())
    val jobDetails = JobDetails(configuration, JobDetails.Source.Http)

    val future = distributor.ask(jobDetails)(timeout = 1.minute)
    val request = future.mapTo[JobDetails].map(details => {
      val result = details.jobResult.getOrElse(Right("Empty result"))
      result match {
        case Left(payload: JobResponse) =>
          JobResult.success(payload, configuration)
        case Right(error: String) =>
          JobResult.failure(error, configuration)
      }
    }).recover {
      case _: AskTimeoutException =>
        JobResult.failure("Job timeout error", configuration)
      case error: Throwable =>
        JobResult.failure(error.getMessage, configuration)
    }

    request.onComplete(_ => distributor ! PoisonPill)

    request
  }

}


class HttpApi(master: MasterService) extends Logger {

  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

  val route: Route = {
    path("internal" / "jobs") {
      get { complete(master.jobsStatuses()) }
    } ~
    path("internal" / "jobs" / Segment) { jobId =>
      delete {
        completeU { master.stopJob(jobId) }
      }
    } ~
    path("internal" / "workers") {
      get { complete(master.workers()) }
    } ~
    path("internal" / "workers") {
      delete { completeU {
        logger.info("STOP ALL")
        master.stopAllWorkers()
      }}
    } ~
    path("internal" / "workers" / Segment) { workerId =>
      delete {
        completeU {
          logger.info("STOP CERTAIN")
          master.stopWorker(workerId)
        }
      }
    } ~
    path("internal" / "routers") {
      get {
        complete {
          val result = master.listRoutes()
           .map(i => i.definition.name -> toHttpRouteInfo(i))
           .toMap
          result
        }
      }
    } ~
    path("api" / Segment) { jobId =>
      post { parameters('train.?, 'serve.?) { (train, serve) =>
        entity(as[JobParameters]) { jobParams =>
          complete {
            val action = if (train.isDefined)
              Action.Train
            else if (serve.isDefined)
              Action.Serve
            else
              Action.Execute

            master.startJob(jobId, action, jobParams)
          }
        }
      }}
    }

  }

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

  private def toHttpRouteInfo(info: JobInfo): HttpJobInfo = info match {
    case py: PyJobInfo => HttpJobInfo.forPython()
    case jvm: JvmJobInfo =>
      val inst = jvm.jobClass
      val classes = inst.supportedClasses()
      HttpJobInfo(
        execute = inst.execute.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),
        train = inst.train.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),
        serve = inst.serve.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),

        isHiveJob = classes.contains(classOf[HiveSupport]),
        isSqlJob = classes.contains(classOf[SQLSupport]),
        isStreamingJob = classes.contains(classOf[StreamingSupport]),
        isMLJob = classes.contains(classOf[MLMistJob])
      )

  }
}
