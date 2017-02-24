package io.hydrosphere.mist

import java.util.concurrent.Executors._
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.StopAllContexts
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.async.mqtt.{MqttSubscriber$, MQTTSubscribe}
import io.hydrosphere.mist.master.{ClusterManager, HttpService, JobRecovery}
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

class ClusterManagerTest extends WordSpecLike with Eventually with BeforeAndAfterAll with ScalaFutures
  with Matchers with JobConfigurationJsonSerialization with DefaultJsonProtocol with HttpService {

  val systemM = ActorSystem("mist", MistConfig.Akka.Main.settings)
  val systemW = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  override implicit val system = systemM
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  Http().bindAndHandle(route, MistConfig.Http.host, MistConfig.Http.port)
  val clientHTTP = Http(systemM)

  val mqttActor = systemM.actorOf(Props(classOf[MqttSubscriber]))
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

  val testTimeout: Duration = 30.seconds

  var success = false

  def httpAsserter(httpMethod: HttpMethod, uri: Uri, request: String, statusCode: StatusCode, messageContains: Option[String] = None): Unit = {
    success = false
    val httpRequest = HttpRequest(httpMethod, uri, entity = HttpEntity(MediaTypes.`application/json`, request))
    val future = clientHTTP.singleRequest(httpRequest)
    future onComplete {
      case Success(msg) => msg match {
        case  HttpResponse(statusCode, _, _, _) => {
          println(msg)
          success = if (messageContains.nonEmpty) {
            msg.entity.toString.contains(messageContains.get)
          } else {
            true
          }
        }
        case _ => println(msg.entity.toString)
      }
      case Failure(e) =>
        println(e)
    }
  }

  override  def beforeAll() = {
    Thread.sleep(5000)
    AddressAndSuccessForWorkerTest.serverAddress = Cluster(systemM).selfAddress.toString
    AddressAndSuccessForWorkerTest.serverName = "/user/" + Constants.Actors.clusterManagerName
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
        systemM.actorOf(Props[ClusterManager], name = Constants.Actors.clusterManagerName)
        Thread.sleep(5000)
        lazy val configurationRepository: JobRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository
        }
        systemM.actorOf(Props(classOf[JobRecovery], configurationRepository), name = "RecoveryActor")

        val workerTestActor = systemW.actorOf(Props[ActorForWorkerTest], name = "TestActor")

        systemW.actorOf(ContextNode.props("foo"), name = "foo")
        Thread.sleep(5000)

        val future = workerTestActor.ask(WorkerIsUp)(timeout = 1.day)
        success = false
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
          success = false
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
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestBad, BadRequest, None)
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "http bad patch" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestBadPath, OK, Option("false"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP bad JSON" in {
        httpAsserter(POST, uri = TestConfig.httpUrl, TestConfig.requestBadJson, BadRequest, None)
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Spark Context jar" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestJar, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Spark Context hdfs jar" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestHdfsJar, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP error in python" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestPyError, OK, Option("false"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Pyspark Context" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestPyspark, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP SparkSQL" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestSparkSql, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Python SparkSQL" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestPysparkSql, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestSparkhive, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP Python Spark HIVE" in {
        if(checkSparkSessionLogic)
          cancel("Can't run in Spark 2.0.0")

        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestPysparkHive, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }

      "HTTP successful restificated request" in {
        httpAsserter(POST, TestConfig.restificatedUrl, TestConfig.restificatedRequest, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
        }
      }
      "HTTP Python hdfs" in {
        httpAsserter(POST, TestConfig.httpUrl, TestConfig.requestPyHdfs, OK, Option("true"))
        eventually(timeout(testTimeout), interval(1 second)) {
          assert(success)
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
            workerManager ! StopAllContexts()
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