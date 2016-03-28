package com.provectus.mist.test

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.server._
import akka.pattern.AskTimeoutException
import com.provectus.mist.MistConfig.Contexts
import com.provectus.mist.jobs.{JobResult, JobConfiguration}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.concurrent.ScalaFutures

import java.net.{InetAddress, InetSocketAddress}

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.io.{Tcp, IO}
import akka.stream.ActorMaterializer
import akka.actor.{Props, ActorRef, Actor}
import akka.testkit.{TestFSMRef, TestProbe, ImplicitSender, TestKit}
import akka.util.{Timeout, ByteString}

import com.provectus.mist._
import com.provectus.mist.actors.tools.Messages.{CreateContext, StopAllContexts}
import com.provectus.mist.actors.{ContextManager, MQTTService, HTTPService}
import com.provectus.mist.contexts.{ContextBuilder, NamedContextSpecification, InMemoryContextRepository}

import net.sigusr.mqtt.api._

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

import HttpMethods._
import StatusCodes._

class TestMist extends FunSuite with HTTPService with Eventually {
  override implicit val system = ActorSystem("mist")
  val testsystem = ActorSystem("test-mist")
  val serverHTTP = Http(system)
  val clientHTTP = Http(testsystem)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  val contextManager = system.actorOf(Props[ContextManager], name = Constants.Actors.contextManagerName)
  val contextName: String = MistConfig.Contexts.precreated.head.toString

  test("No context ") {
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

  test("Create context") {

    contextManager ! CreateContext(contextName)

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

  test("Start http") {
    var http_success = false
    if (MistConfig.HTTP.isOn) {
      val futureHttpServer = serverHTTP.bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
      futureHttpServer onComplete {

        case Success(msg) => {
          println(msg)
          http_success = true
        }

        case Failure(e) => {
          println(e)
          http_success = false
        }
      }
      Await.result(futureHttpServer, 5.seconds)
    }

    eventually(timeout(5 seconds), interval(1 second)) {
      assert(http_success)
    }
  }

  test("Http bad request") {
    var http_response_success = false
    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_bad)))
    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(BadRequest, _, _, _) => {
          http_response_success = true
        }

        case _ => {
          println(msg)
          http_response_success = false
        }
      }

      case Failure(e) => {
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }
    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("Http request spark") {
    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("Http request pyspark") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pyspark)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }

  }

  test("Http request sparksql") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparksql)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }


  test("Http request pysparksql") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparksql)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }

  }

  test("Http request sparkhive") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_sparkhive)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  test("Http request pysparkhive") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_pysparkhive)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 60.seconds)
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }

  system.actorOf(Props[MQTTService])
  val subscriber = system.actorOf(Props[MQTTTestSub])
  val publisher = system.actorOf(Props[MQTTTestPub])

  test("MQTT sub") {
    eventually(timeout(5 seconds), interval(1 second)) {
      assert(MqttSuccessObj.ready_sub)
    }
  }

  test("MQTT pub") {
    eventually(timeout(5 seconds), interval(1 second)) {
      assert(MqttSuccessObj.ready_pub)
    }
  }

  test("MQTT jar") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_jar
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT sql") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_sparksql
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT py") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pyspark
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT pysql") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pysparksql
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT hive") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_sparkhive
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("MQTT pyhive") {
    MqttSuccessObj.success = false
    publisher ! TestConfig.request_pysparkhive
    eventually(timeout(60 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
    }
  }

  test("Multi context") {
    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_jar_other_context)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }
  }
  test("TimeoutException") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_test_timeout)))

    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(OK, _, _, _) => {
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          val errmsg = msg.entity.toString.split(':').drop(3).head.split(',').headOption.getOrElse("")

          if (json == "false" && errmsg == "[\"Job timeout error\"]")
            http_response_success = true
        }
        case _ => {
          println(msg)
          http_response_success = false
        }
      }

      case Failure(e) => {
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }

  }

  test("Error in executer code") {

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.request_testerror)))

    fresponse onComplete {

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
        println("<<  Failure  >>")
        println(e)
        http_response_success = false
      }
    }

    Await.result(fresponse, 10.seconds)
    eventually(timeout(10 seconds), interval(1 second)) {
      assert(http_response_success)
    }

  }

  test("Stop context"){
    var stop_context_success = false
    contextManager ! StopAllContexts

    system.stop(jobRequestActor)
    system.stop(contextManager)
    clientHTTP.shutdownAllConnectionPools().onComplete{ _ =>
      testsystem.shutdown()
    }
    serverHTTP.shutdownAllConnectionPools().onComplete{ _ =>
      system.shutdown()
    }

    eventually(timeout(3 seconds), interval(1 second)) {
      InMemoryContextRepository.get(new NamedContextSpecification(contextName)) match {
        case Some(contextWrapper) => println(contextWrapper)
        case None => stop_context_success = true
      }
      assert(stop_context_success)
    }
  }
  test("Invoking head on an empty Set should produce NoSuchElementException") {
    intercept[NoSuchElementException] {
      Set.empty.head
    }
  }
}

