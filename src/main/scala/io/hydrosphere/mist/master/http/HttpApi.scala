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
import io.hydrosphere.mist.master.{MasterService, JobRoutes, JobDispatcher, WorkerLink}
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

class HttpApi(master: MasterService) extends Logger {

  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

  val route: Route = {
    path("internal" / "jobs") {
      get { complete {
        val exectionStatuses = master.activeJobs()
          .map(details => {
            details.jobId -> JobExecutionStatus(details.startTime, details.endTime, details.status)
          }).toMap
        exectionStatuses
      }}
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
          val result = master.listRoutesInfo()
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
