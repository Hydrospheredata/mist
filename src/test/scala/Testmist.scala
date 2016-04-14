package io.hydrosphere.mist.test

import io.hydrosphere.mist.jobs.{ErrorWrapper, JobByIdSpecification, InMemoryJobRepository}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import org.scalatest._
import org.scalatest.concurrent._

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer

import io.hydrosphere.mist._
import io.hydrosphere.mist.actors.tools.Messages.{RemoveContext, StopAllContexts}
import io.hydrosphere.mist.actors.{HTTPService}
import io.hydrosphere.mist.contexts.{DummyContextSpecification, NamedContextSpecification, InMemoryContextRepository}

import spray.json._

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.{Await}
import scala.concurrent.ExecutionContext.Implicits.global

import HttpMethods._
import StatusCodes._

class TestMist extends FunSuite with HTTPService with Eventually {

  override implicit val system = ActorSystem("test-mist")
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  val testsystem = ActorSystem("test-mist")
  val clientHTTP = Http(testsystem)

  val contextName: String = MistConfig.Contexts.precreated.head.toString

  test("Spark Context is not running") {
    var no_context_success = false
    InMemoryContextRepository.get(new NamedContextSpecification(contextName)) match {

      case Some(contextWrapper) => {
        println(contextWrapper)
        println(contextWrapper.sqlContext.toString)
        no_context_success = false
      }
      case None => no_context_success = true
    }
    assert(no_context_success)
  }

  test("HTTP Server is not running") {
    intercept[akka.stream.StreamTcpException] {
      var http_response_failure = false
      val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_bad)))

      future_response onComplete {

        case Success(msg) => msg match {
          case _ => {
            println(msg)
            http_response_failure = false
          }
        }
        case Failure(e) => {
          println(e)
          http_response_failure = true
        }
      }

      Await.result(future_response, 5.seconds)
      eventually(timeout(5 seconds), interval(1 second)) {
        assert(http_response_failure)
      }
    }
  }

  test("Start Mist"){
    Mist.main(Array(""))
    assert(true)
  }

  test("Spark context launched") {
    var context_success = false
    eventually(timeout(8 seconds), interval(1 second)) {
      InMemoryContextRepository.get(new NamedContextSpecification(contextName)) match {
        case Some(contextWrapper) => {
          println(contextWrapper)
          println(contextWrapper.sqlContext.toString)
          context_success = true
        }
        case None => context_success = false
      }
      assert(context_success)
    }
  }

  test("HTTP bad request") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_bad)))

    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(BadRequest, _, _, _) => {
          println(msg)
          http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad patch") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badpatch)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "false")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad JSON") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badjson)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(BadRequest, _, _, _) => {
          println(msg)
          http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }

    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP bad extension in patch") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_badextension)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(4).head.split(',').head.split('"').headOption.getOrElse("")
          if (json == "false" && errmsg == (s" ${Constants.Errors.extensionError}"))
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP noDoStuff in jar") {

    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_nodostuff)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "false")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Spark Context jar") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP error in python") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyerror)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "false" )
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Pyspark Context") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyspark)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP SparkSQL") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparksql)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Python SparkSQL") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparksql)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Spark HIVE") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparkhive)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Python Spark HIVE") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparkhive)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }


  val subscriber = system.actorOf(Props[MQTTTestSub])
  val publisher = system.actorOf(Props[MQTTTestPub])

  test("MQTT subsctriber") {
    eventually(timeout(5 seconds), interval(1 second)) {
      assert(MqttSuccessObj.ready_sub)
    }
  }

  test("MQTT publisher") {
    eventually(timeout(5 seconds), interval(1 second)) {
      assert(MqttSuccessObj.ready_pub)
    }
  }

  test("MQTT Spark Context jar") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_jar
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Spark SQL") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_sparksql
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Pyspark Context") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pyspark
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Python SQL") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pysparksql
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Spark HIVE") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_sparkhive
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT Python Spark HIVE") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pysparkhive
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT error in Python") {
    MqttSuccessObj.success = true
    publisher ! TestConfig.request_pyerror
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT bad path") {
    MqttSuccessObj.success = true
    publisher ! TestConfig.request_badpatch
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT noDoStuff in jar") {
    MqttSuccessObj.success = true
    publisher ! TestConfig.request_nodostuff
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("MQTT bad JSON") {
    MqttSuccessObj.success = true
    publisher ! TestConfig.request_badjson
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT bad extension in path") {
    MqttSuccessObj.success = true
    publisher ! TestConfig.request_badextension
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(!MqttSuccessObj.success)
    }
  }

  test("HTTP Timeout Exception") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_test_timeout)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(3).head.split(',').headOption.getOrElse("")
          val comperr = "[\"" + Constants.Errors.jobTimeOutError + "\"]"
          if (json == "false" && errmsg == comperr)
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Exception in jar code") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_testerror)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(4).head.split('\"').headOption.getOrElse("")
          println(errmsg)
          if (json == "false" && errmsg == " Test Error")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("HTTP Multi Spark Context") {
    var http_response_success = false
    val future_response = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar_other_context)))
    future_response onComplete {
      case Success(msg) => msg match {
        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if (json == "true")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }
      case Failure(e) => {
        println(e)
        http_response_success = false
      }
    }
    Await.result(future_response, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
  test("MQTT Spark Context jar in Disposable context ") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_jar_disposable_context
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("Test Disposable context after use") {
    var disposable_context_live = false
    eventually(timeout(3 seconds), interval(1 second)) {
      InMemoryContextRepository.get(new NamedContextSpecification(TestConfig.disposable_context_name)) match {
        case Some(contextWrapper) => println(contextWrapper)
        case None => disposable_context_live = true
      }
      assert(disposable_context_live)
    }
  }

  test("Remove other Context"){
    var remove_context_success = false
    var other_context_live = false
    InMemoryContextRepository.get(new NamedContextSpecification(TestConfig.other_context_name)) match {
      case Some(contextWrapper) => {
        println(contextWrapper)
        other_context_live = true
      }
      case None =>
    }

    InMemoryContextRepository.get(new NamedContextSpecification(TestConfig.other_context_name)) match {
      case Some(contextWrapper) => {
        Mist.contextManager ! RemoveContext(contextWrapper)
        InMemoryContextRepository.remove(contextWrapper)
      }
      case None =>
    }

    eventually(timeout(3 seconds), interval(1 second)) {
      InMemoryContextRepository.get(new NamedContextSpecification(TestConfig.other_context_name)) match {
        case Some(contextWrapper) => println(contextWrapper)
        case None => remove_context_success = true
      }
      assert(remove_context_success && other_context_live)
    }
  }

  test("Stop All Contexts"){

    Mist.contextManager ! StopAllContexts

    eventually(timeout(10 seconds), interval(1 second)) {

      var stop_context_success = true
      for (contextWrapper <- InMemoryContextRepository.filter(new DummyContextSpecification())) {
        println(contextWrapper)
        stop_context_success = false
      }
      assert(stop_context_success)
    }

    clientHTTP.shutdownAllConnectionPools().onComplete{ _ =>
      testsystem.shutdown()
    }

    system.stop(jobRequestActor)
    system.stop(subscriber)
    system.stop(publisher)
    system.shutdown()

    Mist.system.stop(Mist.contextManager)
    Mist.system.shutdown()

  }

  test("MistJob"){
    object TestDoStuff extends MistJob {
      override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        Map("result" -> numbers)
      }
      override def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        Map("result" -> numbers)
      }

      override def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = {
        val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
        Map("result" -> numbers)
      }
    }
    if(TestDoStuff != null)
      assert(true)
    else
      assert(false)
  }

  test("AnyJsonFormat read") {
    if(
      5 == AnyJsonFormat.read(JsNumber(5)) &&
      "TestString" == AnyJsonFormat.read(JsString("TestString")) &&
      Map.empty[String, JsValue] == AnyJsonFormat.read(JsObject(Map.empty[String, JsValue])) &&
      true == AnyJsonFormat.read(JsTrue) &&
      false == AnyJsonFormat.read(JsFalse)
    )
      assert(true)
    else
      assert(false)
  }

  test("AnyJsonFormat write") {
    if(
        JsNumber(5) == AnyJsonFormat.write(5) &&
        JsString("TestString") == AnyJsonFormat.write("TestString") &&
        JsArray(JsNumber(1), JsNumber(1), JsNumber(2)) == AnyJsonFormat.write(Seq(1, 1, 2)) &&
        JsObject(Map.empty[String, JsValue]) == AnyJsonFormat.write(Map.empty[String, JsValue]) &&
        JsTrue == AnyJsonFormat.write(true) &&
        JsFalse == AnyJsonFormat.write(false)
    )
      assert(true)
    else
      assert(false)
  }

  test("ErrorWrapper"){
    ErrorWrapper.set("TestUUID", "TestError")
    if("TestError" == ErrorWrapper.get("TestUUID"))
      assert(true)
    else
      assert(false)
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

  test("Constants Errors and Actors"){
      if( Constants.Errors.jobTimeOutError == "Job timeout error"
       && Constants.Errors.noDoStuffMethod == "No overridden doStuff method"
       && Constants.Errors.notJobSubclass == "External module is not MistJob subclass"
       && Constants.Errors.extensionError == "You must specify the path to .jar or .py file"
       && Constants.Actors.syncJobRunnerName == "SyncJobRunner"
       && Constants.Actors.asyncJobRunnerName == "AsyncJobRunner"
       && Constants.Actors.contextManagerName == "ContextManager"
       && Constants.Actors.mqttServiceName == "MQTTService")
        assert(true)
      else assert(false)
  }

}

