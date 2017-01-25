package io.hydrosphere.mist

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
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

  val timeoutAssert = timeout(90 seconds)

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
    Thread.sleep(5000)
  }

  override def afterAll(): Unit = {
    clientHTTP.shutdownAllConnectionPools() andThen { case _ => {
      TestKit.shutdownActorSystem(system)
    }}

    "./bin/mist stop" !

    StartMist.threadMaster.join()
  }

  var success = false

  def httpAssert(method: HttpMethod, uri: Uri, asserter: (String) => Boolean): Unit = {
    success = false
    val httpRequest = HttpRequest(method, uri)
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          success = asserter(msg.entity.toString)
        }
        case _ => println(msg)
      }
      case Failure(e) =>
        println(e)
    }
    Await.result(future_response, 14.seconds)
    Thread.sleep(1000)
  }
  "UI REST Test" must {
    "list workers" in {
      def asserter(msg: String): Boolean = {
        (msg.contains("streaming1") &&
         msg.contains("streaming3") &&
         msg.contains("streaming2"))
      }

      eventually(timeoutAssert, interval(15 second)) {
        httpAssert(GET, TestConfig.restUIUrlListWorkers, asserter)
        assert(success)
      }
    }

    "list jobs" in {
      def asserter(msg: String): Boolean = {
        (msg.contains("job1") &&
         msg.contains("job2") &&
         msg.contains("job3"))
      }

      httpAssert(GET, TestConfig.restUIUrlListJobs, asserter)
      eventually(timeoutAssert, interval(15 second)) {
        assert(success)
      }
    }

    "list routers" in {
      def asserter(msg: String): Boolean = {
        (msg.contains("streaming-1") &&
         msg.contains("streaming-2") &&
         msg.contains("streaming-3"))
      }

      httpAssert(GET, TestConfig.restUIUrlListRouters, asserter)
      eventually(timeoutAssert, interval(15 second)) {
        assert(success)
      }
    }

    "kill job" in {
      def asserter(msg: String): Boolean = {
        val jobKillMsgRegex = """Job job2\w*is scheduled for shutdown. It may take a while.""".r
        msg match {
          case jobKillMsgRege => true
          case _ => false
        }
      }

      httpAssert(DELETE, TestConfig.restUIUrlListJobs+"/job2", asserter)
      eventually(timeoutAssert, interval(15 second)) {
        assert(success)
      }
    }

    "kill worker" in {
      def asserter(msg: String): Boolean = {
        msg.contains("Worker streaming1 is scheduled for shutdown.")
      }

      httpAssert(DELETE, TestConfig.restUIUrlListWorkers + "/streaming1", asserter)
      eventually(timeoutAssert, interval(15 second)) {
        assert(success)
      }
    }

    "kill all workers" in {
      def asserter(msg: String): Boolean = {
        msg.contains(Constants.CLI.stopAllWorkers)
      }

      httpAssert(DELETE, TestConfig.restUIUrlListWorkers, asserter)
      eventually(timeoutAssert, interval(15 second)) {
        assert(success)
      }
    }
  }
}
