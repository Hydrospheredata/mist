package io.hydrosphere.mist.master

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, PoisonPill}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpEntity, _}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.{JobConfigurationJsonSerialization, JobDetailsJsonSerialization, WorkerLinkJsonSerialization}
import io.hydrosphere.mist.{Constants, MistConfig, RouteConfig}
import io.hydrosphere.mist.utils.TypeAlias._
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.cluster.ClusterManager
import spray.json.{pimpAny, pimpString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.language.reflectiveCalls
import scala.util.{Failure, Success}

/** HTTP interface */
private[mist] trait HttpService extends Directives with SprayJsonSupport with JobConfigurationJsonSerialization with WorkerLinkJsonSerialization with JobDetailsJsonSerialization with Logger {

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer

  // /jobs
  def route = {
    path("jobs") {
      // POST /jobs
      post {
        parameters('train.?, 'serve.?) { (train, serve) =>
          entity(as[FullJobConfiguration]) { jobCreatingRequest =>
            val jobConfiguration = FullJobConfigurationBuilder()
              .init(jobCreatingRequest)
              .setServing(serve.nonEmpty)
              .setTraining(train.nonEmpty)
              .build()
            doComplete(jobConfiguration)
          }
        }
      }
    } ~
      pathPrefix("api" / Segment) { jobRoute =>
        pathEnd {
          post {
            parameters('train.?, 'serve.?) { (train, serve) =>
              entity(as[JobParameters]) { jobRequestParams =>
                try {
                  val jobConfiguration = FullJobConfigurationBuilder()
                    .fromRest(jobRoute, jobRequestParams)
                    .setServing(serve.nonEmpty)
                    .setTraining(train.nonEmpty)
                    .build()
                  doComplete(jobConfiguration)
                } catch {
                  case _: RouteConfig.RouteNotFoundError =>
                    complete(HttpResponse(StatusCodes.BadRequest, entity = "Job route config is not valid. Unknown resource!"))
                  case err: Throwable => complete(HttpResponse(StatusCodes.InternalServerError, entity = err.toString))
                }
              }
            }
          }
        }
      } ~
      path("internal" / Segment) { cmd =>
        pathEnd {
          get {
            cmd match {
              case "jobs" => complete {
                HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), JobRepository().filteredByStatuses(List(JobDetails.Status.Running, JobDetails.Status.Queued)).toJson.compactPrint))
              }
              case "workers" => complete {
                implicit val timeout = Timeout.durationToTimeout(Constants.CLI.timeoutDuration)
                val future = system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}") ? ClusterManager.GetWorkers()
                future.recover {
                  case error: Throwable =>
                    HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), error.toString))
                }
                  .map[ToResponseMarshallable] {
                  case list: List[WorkerLink] => HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), list.toJson.compactPrint))
                }
              }
              case "routers" => complete {
                HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), RouteConfig.info.toJson.compactPrint))
              }
            }
          }
        }
      } ~
      pathPrefix("ui") {
        get {
          pathEnd {
            redirect("/ui/", StatusCodes.PermanentRedirect)
          } ~
            pathSingleSlash {
              getFromResource("web/index.html")
            } ~
            getFromResourceDirectory("web")
        }
      } ~
      path("internal" / "jobs" / Segment) { cmd =>
        pathEnd {
          delete {
            doInternalRequestComplete(StopJob(cmd))
          }
        }
      } ~
      path("internal" / "workers" / Segment) { cmd =>
        pathEnd {
          delete {
            doInternalRequestComplete(StopWorker(cmd))
          }
        }
      } ~
      path("internal" / "workers") {
        delete {
          doInternalRequestComplete(StopAllWorkers())
        }
      }
  }

  def doComplete(jobRequest: FullJobConfiguration): akka.http.scaladsl.server.Route = {

    respondWithHeader(RawHeader("Content-Type", "application/json"))

    complete {
      logger.info(jobRequest.parameters.toString)

      val distributor = system.actorOf(JobDispatcher.props())

      val timeDuration = MistConfig.Contexts.timeout(jobRequest.namespace)
      val jobDetails = JobDetails(jobRequest, JobDetails.Source.Http)
      if (timeDuration.isFinite()) {
        val future = distributor.ask(jobDetails)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS))

        future
          .recover {
            case _: AskTimeoutException => Right("Job timeout error")
            case error: Throwable => Right(error.toString)
          }
          .map[ToResponseMarshallable] {
          case jobDetails: JobDetails =>
            val jobResult: JobResult = jobDetails.jobResult.getOrElse(Right("Empty result")) match {
              case Left(jobResults: JobResponse) =>
                JobResult(success = true, payload = jobResults, request = jobRequest, errors = List.empty)
              case Right(error: String) =>
                JobResult(success = false, payload = Map.empty[String, Any], request = jobRequest, errors = List(error))
            }
            logger.info(jobResult.toString)
            distributor ! PoisonPill
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), jobResult.toJson.compactPrint))
          case Right(error: String) =>
            distributor ! PoisonPill
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), JobResult(success = false, payload = Map.empty[String, Any], request = jobRequest, errors = List(error)).toJson.compactPrint))
        }
      }
      else {
        distributor ! jobDetails
        val jobResult = JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobRequest, errors = List.empty)
        HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), jobResult.toJson.compactPrint))
      }
    }
  }

  def doInternalRequestComplete(cmd: AdminMessage): akka.http.scaladsl.server.Route = {
    respondWithHeader(RawHeader("Content-Type", "application/json"))

    complete {

      val clusterManager = system.actorOf(ClusterManager.props())
      val future = clusterManager.ask(cmd)(timeout = Constants.CLI.timeoutDuration)

      future
        .recover {
          case error: Throwable =>
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), error.toString))
        }
        .map[ToResponseMarshallable] {
        case result: String =>
          clusterManager ! PoisonPill
          HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), Map("result" -> result).toJson.compactPrint))
      }
    }
  }
}