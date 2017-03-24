package  io.hydrosphere.mist

//import java.io.{File, FileInputStream, FileOutputStream}
//
//import akka.actor.ActorSystem
//import akka.http.scaladsl.Http
//import akka.http.scaladsl.model.HttpMethods._
//import akka.http.scaladsl.model.StatusCodes._
//import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, MediaTypes}
//import akka.stream.ActorMaterializer
//import akka.testkit.TestKit
//import io.hydrosphere.mist.jobs.{FullJobConfiguration, MistJobConfiguration}
//import io.hydrosphere.mist.utils.json.{AnyJsonFormatSupport, JobConfigurationJsonSerialization}
//import org.apache.commons.lang.SerializationUtils
//import org.mapdb.{DBMaker, Serializer}
//import org.scalatest.concurrent.Eventually
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import spray.json.{DefaultJsonProtocol, DeserializationException, pimpString}
//
//import scala.concurrent.Await
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration.DurationInt
//import scala.sys.process._
//import scala.util.{Failure, Success}
//import spray.json._
//
//import scala.language.postfixOps
//
//class IntegrationTests extends FunSuite with Eventually with BeforeAndAfterAll with AnyJsonFormatSupport
//  with JobConfigurationJsonSerialization with DefaultJsonProtocol{
//
//  implicit val system = ActorSystem("test-mist")
//  implicit val materializer: ActorMaterializer = ActorMaterializer()
//
//  val clientHTTP = Http(system)
//
//  val contextName: String = MistConfig.Contexts.precreated.headOption.getOrElse("foo")
//
//  object StartMist {
//    val threadMaster = {
//      new Thread {
//        override def run() = {
//          s"./bin/mist start master --config ${TestConfig.integrationConf}" !
//        }
//      }
//    }
//  }
//
//  override def beforeAll(): Unit = {
//    Thread.sleep(5000)
//    if (MistConfig.Recovery.recoveryOn) {
//      val db = DBMaker
//        .fileDB(MistConfig.Recovery.recoveryDbFileName + "b")
//        .make
//
//      // Map
//      val map = db
//        .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
//        .createOrOpen
//
//      val stringMessage = TestConfig.requestJar
//      val json = stringMessage.parseJson
//      val jobCreatingRequest = json.convertTo[MistJobConfiguration]
//      val w_job = jobCreatingRequest.toJson.compactPrint.getBytes
//      map.clear()
//      for (i <- 1 to 3) {
//        map.put("3e72eaa8-682a-45aa-b0a5-655ae8854c" + i.toString, w_job)
//      }
//
//      map.close()
//      db.close()
//
//      val src = new File(MistConfig.Recovery.recoveryDbFileName + "b")
//      val dest = new File(MistConfig.Recovery.recoveryDbFileName)
//      new FileOutputStream(dest) getChannel() transferFrom(
//        new FileInputStream(src) getChannel, 0, Long.MaxValue)
//    }
//
//    MQTTTest.subscribe(system)
//
//    StartMist.threadMaster.start()
//
//    Thread.sleep(30000)
//  }
//
//  test("HTTP bad request") {
//
//    var http_response_success = false
//    val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrlIt,
//      entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBad))
//    val future_response = clientHTTP.singleRequest(httpRequest)
//
//    future_response onComplete {
//      case Success(msg) => msg match {
//        case HttpResponse(BadRequest, _, _, _) =>
//          println(msg)
//          http_response_success = true
//        case _ =>
//          println(msg)
//          http_response_success = false
//      }
//      case Failure(e) =>
//        println(e)
//        http_response_success = false
//    }
//    Await.result(future_response, 30.seconds)
//    eventually(timeout(30 seconds), interval(1 second)) {
//      assert(http_response_success)
//    }
//  }
//
//  test("HTTP bad path") {
//    var http_response_success = false
//    val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrlIt,
//      entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBadPath))
//    val future_response = clientHTTP.singleRequest(httpRequest)
//    future_response onComplete {
//      case Success(msg) => msg match {
//        case HttpResponse(OK, _, _, _) =>
//          println(msg)
//          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
//          if (json == "false") {
//            http_response_success = true
//          }
//        case _ =>
//          println(msg)
//          http_response_success = false
//      }
//      case Failure(e) =>
//        println(e)
//        http_response_success = false
//    }
//    Await.result(future_response, 30.seconds)
//    eventually(timeout(30 seconds), interval(1 second)) {
//      assert(http_response_success)
//    }
//  }
//
//  test("HTTP bad JSON") {
//    var http_response_success = false
//    val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrlIt,
//      entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBadJson))
//    val future_response = clientHTTP.singleRequest(httpRequest)
//    future_response onComplete {
//      case Success(msg) => msg match {
//        case HttpResponse(BadRequest, _, _, _) =>
//          println(msg)
//          http_response_success = true
//        case _ =>
//          println(msg)
//          http_response_success = false
//      }
//      case Failure(e) =>
//        println(e)
//        http_response_success = false
//    }
//
//    Await.result(future_response, 30.seconds)
//    eventually(timeout(30 seconds), interval(1 second)) {
//      assert(http_response_success)
//    }
//  }
//
//  test("HTTP Spark Context jar") {
//    var http_response_success = false
//    val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrlIt,
//      entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestJar))
//    val future_response = clientHTTP.singleRequest(httpRequest)
//    future_response onComplete {
//      case Success(msg) => msg match {
//        case HttpResponse(OK, _, _, _) =>
//          println(msg)
//          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
//          http_response_success = json == "true"
//        case _ =>
//          println(msg)
//          http_response_success = false
//      }
//      case Failure(e) =>
//        println(e)
//        http_response_success = false
//    }
//    Await.result(future_response, 30.seconds)
//    eventually(timeout(30 seconds), interval(1 second)) {
//      assert(http_response_success)
//    }
//  }
//
//  override def afterAll(): Unit ={
//
//    "./bin/mist stop" !
//
//    TestKit.shutdownActorSystem(system)
//
//    StartMist.threadMaster.join()
//
//    Thread.sleep(5000)
//  }
//}
