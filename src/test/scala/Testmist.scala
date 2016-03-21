package com.provectus.mist.test

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
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.ExecutionContext.Implicits.global

import HttpMethods._
import StatusCodes._

class TestMist extends FunSuite with HTTPService with Eventually{
  override implicit val system = ActorSystem("mist")
  val testsystem = ActorSystem("test-mist")
  val serverHTTP = Http(system)
  val clientHTTP = Http(testsystem)
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  val contextManager = system.actorOf(Props[ContextManager], name = Constants.Actors.contextManagerName)
  val contextName: String =  MistConfig.Contexts.precreated.head.toString

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

  test("Http request spark") {
    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.http_request_jar)))

    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(OK, _, _, _) => {
          println("***")
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if(json == "true")
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

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.http_request_pyspark)))

    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(OK, _, _, _) => {
          println("***")
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if(json == "true")
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

  test("Http request sparksql"){

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.http_request_sparksql) ))

    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(OK, _, _, _) => {
          println("***")
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if(json == "true")
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
  test("Http request pysparksql"){

    var http_response_success = false

    val fresponse = clientHTTP.singleRequest(HttpRequest(POST, uri = TestConfig.http_url, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.http_request_pysparksql) ))

    fresponse onComplete {

      case Success(msg) => msg match {

        case HttpResponse(OK, _, _, _) => {
          println("***")
          println(msg)
          val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false")
          if(json == "true")
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

  test("MQTT") {

    system.actorOf(Props[MQTTService])
    val subscriber = system.actorOf(Props[MQTTTestSub])
    val publisher = system.actorOf(Props[MQTTTestPub])

    Thread.sleep(3000)

    publisher ! TestConfig.mqtt_request_jar

    println(MqttSuccessObj.success)
    eventually(timeout(8 seconds), interval(1 second)) {
      assert(MqttSuccessObj.success)
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
}

