package io.hydrosphere.mist

import java.io.{File, FileInputStream, FileOutputStream}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods.POST
import akka.http.scaladsl.model.StatusCodes.{OK, BadRequest}
import akka.http.scaladsl.model.{HttpRequest, HttpEntity, HttpResponse, MediaTypes}
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.Messages.StopAllContexts
import io.hydrosphere.mist.contexts.{DummyContextSpecification, InMemoryContextRepository, NamedContextSpecification}
import io.hydrosphere.mist.jobs.{ErrorWrapper, Job, JobConfiguration, RecoveryJobRepository}
import io.hydrosphere.mist.master.JsonFormatSupport
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}
import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.scalatest.concurrent.Eventually
import spray.json.{DefaultJsonProtocol, DeserializationException, pimpString, JsNumber, JsString, JsTrue, JsValue, JsFalse, JsArray, JsNull, JsObject}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}
// scalastyle:off
import sys.process._
// scalastyle:on


/* @Ignore */ class Testmist extends FunSuite with Eventually with DefaultJsonProtocol with JsonFormatSupport with BeforeAndAfterAll {

  implicit val system = ActorSystem("test-mist")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val testSystem = ActorSystem("test-mist")
  val clientHTTP = Http(testSystem)

  val contextName: String = MistConfig.Contexts.precreated.headOption.getOrElse("foo")

  override def beforeAll(): Unit = {
    // prepare recovery file
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
  }

  test("Spark Context is not running") {
    var no_context_success = false
    InMemoryContextRepository.get(new NamedContextSpecification(contextName)) match {

      case Some(contextWrapper) =>
        println(contextWrapper)
        println(contextWrapper.sqlContext.toString)
        no_context_success = false
      case None => no_context_success = true
    }
    assert(no_context_success)
  }

  test("Recovery 3 jobs from MapDb") {

    if (!MistConfig.Recovery.recoveryOn) {
      Master.main(Array(""))
      /*
        new Thread {
          override def run() = {
            "./mist.sh master" !
          }
        }.start()
*/
      cancel("Can't run the Recovery test because recovery off in config file")
    }
    else {

      Master.main(Array(""))
/*
      new Thread {
        override def run() = {
          "./mist.sh master" !
        }
      }.start()
*/
      var jobidSet = Set.empty[String]

      val jobRepository = RecoveryJobRepository

      eventually(timeout(90 seconds), interval(500 milliseconds)) {
        jobRepository.filter(new Specification[Job] {
          override def specified(element: Job): Boolean = true
        }).foreach(x => {
          jobidSet = jobidSet + x.id
        })
        assert(jobidSet.size == 3)
      }

    }
  }
/*
  test("HTTP bad request") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_bad))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badpatch))
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
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad JSON") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badjson))
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

  test("HTTP noDoStuff in jar") {

    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_nodostuff))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar))
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

  test("HTTP error in python") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyerror))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyspark))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparksql))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparksql))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparkhive))
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
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparkhive))
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

  test("HTTP Timeout Exception") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_test_timeout))
    val future_response = clientHTTP.singleRequest(httpRequest)
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) =>
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(3).head.split(',').headOption.getOrElse("")
          val comperr = "[\"" + Constants.Errors.jobTimeOutError + "\"]"
          http_response_success = json == "false" && errmsg == comperr
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

  test("Spark context launched") {

    var context_success = false
    eventually(timeout(190 seconds), interval(1 second)) {
      InMemoryContextRepository.get(new NamedContextSpecification(contextName)) match {
        case Some(contextWrapper) =>

          context_success = true
        case None => context_success = false
      }
      assert(context_success)
    }
  }

  test("HTTP Exception in jar code") {
    var http_response_success = false
    val httpRequest = HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_testerror))
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
  test("Stop All Contexts") {

    Master.workerManager ! StopAllContexts

    eventually(timeout(10 seconds), interval(1 second)) {

      var stop_context_success = true
      for (contextWrapper <- InMemoryContextRepository.filter(new DummyContextSpecification())) {
        println(contextWrapper)
        stop_context_success = false
      }
      assert(stop_context_success)
    }

    clientHTTP.shutdownAllConnectionPools().onComplete { _ =>
      testSystem.shutdown()
    }

    Master.system.stop(Master.workerManager)
    Master.system.shutdown()

  }

  test("AnyJsonFormat read") {
    assert(
      5 == AnyJsonFormat.read(JsNumber(5)) &&
        "TestString" == AnyJsonFormat.read(JsString("TestString")) &&
        Map.empty[String, JsValue] == AnyJsonFormat.read(JsObject(Map.empty[String, JsValue])) &&
        true == AnyJsonFormat.read(JsTrue) &&
        false == AnyJsonFormat.read(JsFalse)
    )
  }

  test("AnyJsonFormat write") {
    assert(
      JsNumber(5) == AnyJsonFormat.write(5) &&
        JsString("TestString") == AnyJsonFormat.write("TestString") &&
        JsArray(JsNumber(1), JsNumber(1), JsNumber(2)) == AnyJsonFormat.write(Seq(1, 1, 2)) &&
        JsObject(Map.empty[String, JsValue]) == AnyJsonFormat.write(Map.empty[String, JsValue]) &&
        JsTrue == AnyJsonFormat.write(true) &&
        JsFalse == AnyJsonFormat.write(false)
    )
  }

  test("ErrorWrapper") {
    ErrorWrapper.set("TestUUID", "TestError")
    assert("TestError" == ErrorWrapper.get("TestUUID"))
    ErrorWrapper.remove("TestUUID")
  }

  test("AnyJsonFormat serializationError") {
    intercept[spray.json.SerializationException] {
      val unknown = Set(1, 2)
      AnyJsonFormat.write(unknown)
    }
  }

  test("AnyJsonFormat deserilalizationError") {
    intercept[spray.json.DeserializationException] {
      val unknown = JsNull
      AnyJsonFormat.read(unknown)
    }
  }

  test("Constants Errors and Actors") {
    assert(Constants.Errors.jobTimeOutError == "Job timeout error"
      && Constants.Errors.noDoStuffMethod == "No overridden doStuff method"
      && Constants.Errors.notJobSubclass == "External module is not MistJob subclass"
      && Constants.Errors.extensionError == "You must specify the path to .jar or .py file"
      && Constants.Actors.syncJobRunnerName == "SyncJobRunner"
      && Constants.Actors.asyncJobRunnerName == "AsyncJobRunner"
      && Constants.Actors.workerManagerName == "ContextManager"
      && Constants.Actors.mqttServiceName == "MQTTService")
  }

}
