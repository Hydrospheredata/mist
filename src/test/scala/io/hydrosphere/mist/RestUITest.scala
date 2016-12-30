package io.hydrosphere.mist

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, MediaTypes}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, WordSpecLike, _}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.{Failure, Success}

class RestUITest extends WordSpecLike with BeforeAndAfterAll with Eventually with JobConfigurationJsonSerialization with DefaultJsonProtocol with ScalaFutures with Matchers {

  implicit val system = ActorSystem("test-mist")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val clientHTTP = Http(system)

  object StartMist {
    val threadMaster = {
      new Thread {
        override def run(): Unit = {
          s"./bin/mist start master --config ${TestConfig.restUIConfig}" !
        }
      }
    }
  }

  class StartJob(route: String, externalId: String) {
    val threadMaster = {
      new Thread {
        override def run(): Unit = {
          s"bin/mist start job --config ${TestConfig.restUIConfig} --route ${route} --external-id ${externalId}" !
        }
      }.start()
    }
  }

  object StartJobs {
    val routers = List(("streaming-1", "job1"), ("streaming-2", "job2"), ("streaming-3", "job3"))
    def start(): Unit = {
      routers.foreach {
        router =>
          new StartJob(router._1, router._2)
          Thread.sleep(1000)
      }
    }
  }

  override  def beforeAll(): Unit = {
    StartMist.threadMaster.start()
    Thread.sleep(5000)
    StartJobs.start()
  }

  override def afterAll(): Unit = {
    clientHTTP.shutdownAllConnectionPools() andThen { case _ => {
      TestKit.shutdownActorSystem(system)
    }}

    "./bin/mist stop" !

    StartMist.threadMaster.join()
  }

  "UI REST Test" must {
    "list workers" in {
      var http_response_success = false
      eventually(timeout(180 seconds), interval(10 second)) {
        val httpRequest = HttpRequest(POST, uri = TestConfig.restUIUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestUIListWorkers))
        val future_response = clientHTTP.singleRequest(httpRequest)

        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              if(msg.entity.toString.contains("streaming1") &&
                msg.entity.toString.contains("streaming2") &&
                msg.entity.toString.contains("streaming3")) {
                http_response_success = true
              }
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 10.seconds)
        assert(http_response_success)
      }
    }

    "list jobs" in {
      var http_response_success = false
      eventually(timeout(60 seconds), interval(10 second)) {
        val httpRequest = HttpRequest(POST, uri = TestConfig.restUIUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestUIListJobs))
        val future_response = clientHTTP.singleRequest(httpRequest)

        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              if(msg.entity.toString.contains("job1") &&
                msg.entity.toString.contains("job2") &&
                msg.entity.toString.contains("job3")) {
                http_response_success = true
              }
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 10.seconds)
        assert(http_response_success)
      }
    }

    "kill job" in {
      var http_response_success = false
      eventually(timeout(60 seconds), interval(10 second)) {
        val httpRequest = HttpRequest(POST, uri = TestConfig.restUIUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestUIKillJob))
        val future_response = clientHTTP.singleRequest(httpRequest)

        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val jobKillMsgRegex = """Job job2\w*is scheduled for shutdown. It may take a while.""".r
              msg.entity.toString match {
                case jobKillMsgRege => http_response_success = true
                case _ => http_response_success = false
              }

            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 10.seconds)
        assert(http_response_success)
      }
    }

    "kill worker" in {
      var http_response_success = false
      eventually(timeout(60 seconds), interval(10 second)) {
        val httpRequest = HttpRequest(POST, uri = TestConfig.restUIUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestUIKillWorker))
        val future_response = clientHTTP.singleRequest(httpRequest)

        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              if(msg.entity.toString.contains("Worker streaming1 is scheduled for shutdown.")) {
                http_response_success = true
              }
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 10.seconds)
        assert(http_response_success)
      }
    }

    "kill all" in {
      var http_response_success = false
      eventually(timeout(60 seconds), interval(10 second)) {
        val httpRequest = HttpRequest(POST, uri = TestConfig.restUIUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestUIKillAll))
        val future_response = clientHTTP.singleRequest(httpRequest)

        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              if(msg.entity.toString.contains("All Context is scheduled for shutdown.")) {
                http_response_success = true
              }
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 10.seconds)
        assert(http_response_success)
      }
    }
  }
}
