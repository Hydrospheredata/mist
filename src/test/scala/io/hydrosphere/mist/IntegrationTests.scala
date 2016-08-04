package  io.hydrosphere.mist

import java.io.{File, FileInputStream, FileOutputStream}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, MediaTypes}
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.hydrosphere.mist.jobs.{ConfigurationRepository, InMapDbJobConfigurationRepository, InMemoryJobConfigurationRepository, JobConfiguration}
import io.hydrosphere.mist.master.{JsonFormatSupport, TryRecoveyNext}
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spray.json.{DefaultJsonProtocol, DeserializationException, JsArray, JsFalse, JsNull, JsNumber, JsObject, JsString, JsTrue, JsValue, pimpString}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}
import sys.process._
import scala.io.Source
import org.scalatest._

import scala.util.matching.Regex //for Ignore

@Ignore class IntegrationTests extends FunSuite with Eventually with BeforeAndAfterAll with JsonFormatSupport with DefaultJsonProtocol{

  implicit val system = ActorSystem("test-mist")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  //val testSystem = ActorSystem("test-mist")
  val clientHTTP = Http(system)

  val contextName: String = MistConfig.Contexts.precreated.headOption.getOrElse("foo")
/*
  val versionRegex = "(\\d+)\\.(\\d+).*".r
  val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

  val checkSparkSessionLogic = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt > 1 => true
      case _ => false
    }
  }
*/

  object StartMist {
  val threadMaster = {
    new Thread {
      override def run() = {
        s"./mist.sh master --config configs/integration.conf --jar ${TestConfig.assemblyjar}" !
      }
    }
  }
}

  override def beforeAll(): Unit = {
    Thread.sleep(5000)
    if (MistConfig.Recovery.recoveryOn) {
      val db = DBMaker
        .fileDB(MistConfig.Recovery.recoveryDbFileName + "b")
        .make

      // Map
      val map = db
        .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
        .createOrOpen

      val stringMessage = TestConfig.request_jar
      val json = stringMessage.parseJson
      val jobCreatingRequest = {
        try {
          json.convertTo[JobConfiguration]
        } catch {
          case _: DeserializationException => None
        }
      }
      val w_job = SerializationUtils.serialize(jobCreatingRequest)
      var i = 0
      map.clear()
      for (i <- 1 to 3) {
        map.put("3e72eaa8-682a-45aa-b0a5-655ae8854c" + i.toString, w_job)
      }

      map.close()
      db.close()

      val src = new File(MistConfig.Recovery.recoveryDbFileName + "b")
      val dest = new File(MistConfig.Recovery.recoveryDbFileName)
      new FileOutputStream(dest) getChannel() transferFrom(
        new FileInputStream(src) getChannel, 0, Long.MaxValue)
    }

    MQTTTest.subscribe(system)

    StartMist.threadMaster.start()

    Thread.sleep(10000)
  }


  test("HTTP bad request") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_bad))
    val future_response = clientHTTP.singleRequest(httpRequest)

    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(BadRequest, _, _, _) =>
          println(msg)
          http_response_success = true
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad patch") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badpatch))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "false") {
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
    Await.result(future_response, 20.seconds)
    eventually(timeout(20 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad JSON") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badjson))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(BadRequest, _, _, _) =>
          println(msg)
          http_response_success = true
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }

    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
/*
  test("HTTP bad extension in patch") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badextension))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(4).head.split(',').head.split('"').headOption.getOrElse("")
          if (json == "false" && errmsg == s" ${Constants.Errors.extensionError}") {
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
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
*/
  test("HTTP noDoStuff in jar") {

    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_nodostuff))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "false"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Spark Context jar") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
/*
  test("HTTP error in python") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyerror))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "false"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Pyspark Context") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyspark))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP SparkSQL") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparksql))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Python SparkSQL") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparksql))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Spark HIVE") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparkhive))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Python Spark HIVE") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparkhive))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          http_response_success = json == "true"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  MQTTTest.subscribe(system)

  test("MQTT bad JSON") {
    MqttSuccessObj.success = true
    MQTTTest.publish(TestConfig.request_badjson)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Spark Context jar") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_jar)

    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Spark SQL") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_sparksql)
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Pyspark Context") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_pyspark)
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Python SQL") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_pysparksql)
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Spark HIVE") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_sparkhive)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Python Spark HIVE") {
    MqttSuccessObj.success = false
    MQTTTest.publish(TestConfig.request_pysparkhive)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT error in Python") {
    MqttSuccessObj.success = true
    MQTTTest.publish(TestConfig.request_pyerror)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT bad path") {
    MqttSuccessObj.success = true
    MQTTTest.publish(TestConfig.request_badpatch)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT noDoStuff in jar") {
    MqttSuccessObj.success = true
    MQTTTest.publish(TestConfig.request_nodostuff)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT bad extension in path") {
    MqttSuccessObj.success = true
    MQTTTest.publish(TestConfig.request_badextension)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("HTTP Exception in jar code") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url_it, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_testerror))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(4).head.split('\"').headOption.getOrElse("")
          println(errmsg)
          http_response_success = json == "false" && errmsg == " Test Error"
        case _ =>
          println(msg)
          http_response_success = false
      }
      case Failure(e) =>
        println(e)
        http_response_success = false
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
*/
  override def afterAll(): Unit ={

    val pid = Source.fromFile("master.pid").getLines.mkString
    s"kill ${pid}"!

    TestKit.shutdownActorSystem(system)
    //TestKit.shutdownActorSystem(testSystem)

    StartMist.threadMaster.join()

    Thread.sleep(5000)
  }
}
