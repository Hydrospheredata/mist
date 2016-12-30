package io.hydrosphere.mist

import java.util.concurrent.Executors._
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, MediaTypes}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.StopAllContexts
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.mqtt.{MQTTServiceActor, MQTTSubscribe}
import io.hydrosphere.mist.master.{ClusterManager, HTTPService, JobRecovery}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.worker.ContextNode
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time._
import spray.json.{DefaultJsonProtocol, pimpString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object AddressAndSuccessForWorkerTest {
  var nodeAddress: String = _
  var nodeName: String = _
  var serverAddress: String = _
  var serverName: String = _
  var success: Boolean = false
}

object WorkerIsUp
object WorkerIsRemoved

class ActorForWorkerTest extends Actor with ActorLogging {

  private val cluster = Cluster(context.system)
  private var workerUp = false
  private var workerRemowed = false
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))
  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    context.stop(self)
  }

  override def receive: Receive = {
    case MemberUp(member) =>
      println(s"TestActor: catch Up  ${member.address}", AddressAndSuccessForWorkerTest.nodeAddress)
      if (member.address.toString == AddressAndSuccessForWorkerTest.nodeAddress) {
        workerUp = true
      }
      println("TestActor workerUp:", workerUp)

    case MemberRemoved(member, previousStatus) =>
      println(s"TestActor: catch Removed  ${member.address}", AddressAndSuccessForWorkerTest.nodeAddress)
      if (member.address.toString == AddressAndSuccessForWorkerTest.nodeAddress) {
        workerRemowed = true
      }
      println("TestActor workerRemoved:", workerRemowed)

    case WorkerIsUp =>
      sender ! workerUp

    case WorkerIsRemoved =>
      sender ! workerRemowed

  }
}

class workerManagerTestActor extends WordSpecLike with Eventually with BeforeAndAfterAll with ScalaFutures with Matchers with JobConfigurationJsonSerialization with DefaultJsonProtocol with HTTPService{

  val systemM = ActorSystem("mist", MistConfig.Akka.Main.settings)
  val systemW = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  override implicit val system = systemM
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
  val clientHTTP = Http(systemM)

  val mqttActor = systemM.actorOf(Props(classOf[MQTTServiceActor]))
  mqttActor ! MQTTSubscribe
  MQTTTest.subscribe(systemM)

  val versionRegex = "(\\d+)\\.(\\d+).*".r
  val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

  val checkSparkSessionLogic = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt > 1 => true
      case _ => false
    }
  }

  override  def beforeAll() = {
    Thread.sleep(5000)
    AddressAndSuccessForWorkerTest.serverAddress = Cluster(systemM).selfAddress.toString
    AddressAndSuccessForWorkerTest.serverName = "/user/" + Constants.Actors.workerManagerName
    AddressAndSuccessForWorkerTest.nodeAddress = Cluster(systemW).selfAddress.toString
    AddressAndSuccessForWorkerTest.nodeName = "/user/" + "foo"

  }
  override def afterAll() = {
    clientHTTP.shutdownAllConnectionPools()
    Http().shutdownAllConnectionPools()

    TestKit.shutdownActorSystem(systemM)
    TestKit.shutdownActorSystem(systemW)
    TestKit.shutdownActorSystem(system)

    Thread.sleep(5000)
  }

    "ContextNode" must {
      "started" in {
        systemM.actorOf(Props[ClusterManager], name = Constants.Actors.workerManagerName)
        Thread.sleep(5000)
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository
        }
        systemM.actorOf(Props(classOf[JobRecovery], configurationRepository), name = "RecoveryActor")

        val workerTestActor = systemW.actorOf(Props[ActorForWorkerTest], name = "TestActor")

        systemW.actorOf(ContextNode.props("foo"), name = "foo")
        Thread.sleep(5000)

        val future = workerTestActor.ask(WorkerIsUp)(timeout = 1.day)
        var success = false
        future
            .onSuccess{
              case result:Boolean =>
                success = result
              println("workerUp status", success)
            }
        Await.result(future, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(success)
        }
      }

      "message" in {
        val contextNode = systemW.actorSelection(AddressAndSuccessForWorkerTest.nodeAddress + AddressAndSuccessForWorkerTest.nodeName)
        val json = TestConfig.requestJar.parseJson
        val jobConfiguration = json.convertTo[FullJobConfiguration]
        val timeDuration = MistConfig.Contexts.timeout(jobConfiguration.namespace)
        if(timeDuration.isFinite()) {
          val future = contextNode.ask(jobConfiguration)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS))
          var success = false
          future
            .onSuccess {
              case result: Either[Map[String, Any], String] =>
                val jobResult: JobResult = result match {
                  case Left(jobResult: Map[String, Any]) =>
                    JobResult(success = true, payload = jobResult, request = jobConfiguration, errors = List.empty)
                  case Right(error: String) =>
                    JobResult(success = false, payload = Map.empty[String, Any], request = jobConfiguration, errors = List(error))
                }
                success = jobResult.success
            }

          Await.result(future, 60.seconds)
          eventually(timeout(60 seconds), interval(1 second)) {
            assert(success)
          }
        }
        else
          cancel("Infinite timeout duration")
      }

      "http bad request" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBad))
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
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "http bad patch" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBadPath))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
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

      "HTTP bad JSON" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestBadJson))
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

        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP Spark Context jar" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestJar))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP Spark Context hdfs jar" in {
        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestHdfsJar))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP error in python" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestPyError))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "false"
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

      "HTTP Pyspark Context" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestPyspark))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP SparkSQL" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestSparkSql))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP Python SparkSQL" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestPysparkSql))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "HTTP Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestSparkhive))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
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

      "HTTP Python Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestPysparkHive))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
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

      "HTTP successful restificated request" in {
        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.restificatedUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.restificatedRequest))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
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
      "HTTP Python hdfs" in {

        var http_response_success = false
        val httpRequest = HttpRequest(POST, uri = TestConfig.httpUrl, entity = HttpEntity(MediaTypes.`application/json`, TestConfig.requestPyHdfs))
        val future_response = clientHTTP.singleRequest(httpRequest)
        future_response onComplete {
          case Success(msg) => msg match {
            case HttpResponse(OK, _, _, _) =>
              println(msg)
              val json = msg.entity.toString.split(':').drop(1).head.split(',').headOption.getOrElse("false").trim
              http_response_success = json == "true"
            case _ =>
              println(msg)
              http_response_success = false
          }
          case Failure(e) =>
            println(e)
            http_response_success = false
        }
        Await.result(future_response, 30.seconds)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "mqtt jar" in {

        MqttSuccessObj.success = false
        MQTTTest.publish(TestConfig.requestJar)
        Thread.sleep(5000)

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

      "mqtt spark sql" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        MqttSuccessObj.success = false

        MQTTTest.publish(TestConfig.requestSparkSql)
        Thread.sleep(5000)

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

      "mqtt successful restificated request" in {
        MqttSuccessObj.success = false

        MQTTTest.publish(TestConfig.asyncRestificatedRequest)
        Thread.sleep(5000)

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

//TODO check py sql
      /*
      "mqtt python spark sql" in {
        MqttSuccessObj.success = false

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }
*/
      "mqtt python spark" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        MqttSuccessObj.success = false
        MQTTTest.publish(TestConfig.requestPyspark)
        Thread.sleep(5000)

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }
      
      "MQTT Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        MqttSuccessObj.success = false
        MQTTTest.publish(TestConfig.requestSparkhive)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

      "MQTT Python Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        MqttSuccessObj.success = false
        MQTTTest.publish(TestConfig.requestPysparkHive)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

      "MQTT error in Python" in {
        MqttSuccessObj.success = true
        MQTTTest.publish(TestConfig.requestPyError)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(!MqttSuccessObj.success)
        }
      }

      "MQTT bad path" in {
        MqttSuccessObj.success = true
        MQTTTest.publish(TestConfig.requestBadPath)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(!MqttSuccessObj.success)
        }
      }

      "MQTT bad extension in path" in {
        MqttSuccessObj.success = true
        MQTTTest.publish(TestConfig.requestBadExtension)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(!MqttSuccessObj.success)
        }
      }

      "Python publisher in MQTT" in {
        MqttSuccessObj.successPythonMqttPub = false
        MQTTTest.publish(TestConfig.requestPyMqttPublisher)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.successPythonMqttPub)
        }
      }

      "stopped" in {
        new Thread {
          override def run() = {
            AddressAndSuccessForWorkerTest.success = false
            val workerManager = systemM.actorSelection(AddressAndSuccessForWorkerTest.serverAddress + AddressAndSuccessForWorkerTest.serverName)
            workerManager ! StopAllContexts
            Thread.sleep(5000)
            val workerTestActor = systemW.actorSelection(AddressAndSuccessForWorkerTest.nodeAddress + "/user/TestActor")
            val future = workerTestActor.ask(WorkerIsRemoved)(timeout = 1.day)
            var success = false
            future
              .onSuccess {
                case result: Boolean => {
                  success = result
                }
                  println("workerRemoved status", success)
              }
            Await.result(future, 30.seconds)
            //workerManager ! ShutdownMaster //TODO test shutdown workers after master
            eventually(timeout(30 seconds), interval(1 second)) {
              assert(success)
            }

          }
        }.start()
      }

  }
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(1, Second))
}