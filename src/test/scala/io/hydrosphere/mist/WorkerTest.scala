package  io.hydrosphere.mist


import java.util.concurrent.Executors._

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import io.hydrosphere.mist.Messages.{RemoveContext, ShutdownMaster, StopAllContexts, WorkerDidStart}
import io.hydrosphere.mist.master.{HTTPService, JobRecovery, JsonFormatSupport, WorkerManager}
import io.hydrosphere.mist.worker.ContextNode

import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time._

import scala.concurrent.duration._
import akka.cluster._
import akka.cluster.ClusterEvent._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, MediaTypes}
import akka.stream.ActorMaterializer
import io.hydrosphere.mist.MistConfig.Akka.Main
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.mqtt.{MQTTServiceActor, MqttSubscribe}
import spray.json.{DefaultJsonProtocol, pimpString}

import scala.concurrent.{Await, ExecutionContext}
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
  //var gocha
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))
  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
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

class workerManagerTestActor extends WordSpecLike with Eventually with BeforeAndAfterAll with ScalaFutures with Matchers with JsonFormatSupport with DefaultJsonProtocol with HTTPService{

  val systemM = ActorSystem("mist", MistConfig.Akka.Main.settings)
  val systemW = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  override implicit val system = systemM
  override implicit val materializer: ActorMaterializer = ActorMaterializer()

  override  def beforeAll() = {
    Thread.sleep(5000)
  }
  override def afterAll() = {
    TestKit.shutdownActorSystem(systemM)
    TestKit.shutdownActorSystem(systemW)
    Thread.sleep(5000)
  }

    "ContextNode" must {
      "started" in {
        val workerManager = systemM.actorOf(Props[WorkerManager], name = Constants.Actors.workerManagerName)
        Thread.sleep(5000)
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository
        }
        val recoveryActor = systemM.actorOf(Props(classOf[JobRecovery], configurationRepository), name = "RecoveryActor")
        val workerTestActor = systemW.actorOf(Props[ActorForWorkerTest], name = "TestActor")

        AddressAndSuccessForWorkerTest.serverAddress = Cluster(systemM).selfAddress.toString
        AddressAndSuccessForWorkerTest.serverName = "/user/" + Constants.Actors.workerManagerName
        AddressAndSuccessForWorkerTest.nodeAddress = Cluster(systemW).selfAddress.toString
        AddressAndSuccessForWorkerTest.nodeName = "/user/" + "foo"

        val contextNode = systemW.actorOf(ContextNode.props("foo"), name = "foo")
        Thread.sleep(5000)
        val future = workerTestActor.ask(WorkerIsUp)(timeout = 1.day)
        var success = false
        future
            .onSuccess{
              case result:Boolean => val Result: Boolean = result match {
                case answer: Boolean => answer
              }
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
        val json = TestConfig.request_jar.parseJson
        val jobConfiguration = json.convertTo[JobConfiguration]
        val future = contextNode.ask(jobConfiguration)(timeout = MistConfig.Contexts.timeout(jobConfiguration.name))
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

      "mqtt jar" in {
        MqttSuccessObj.success = false
        val mqttActor = systemM.actorOf(Props(classOf[MQTTServiceActor]))
        mqttActor ! MqttSubscribe
        MQTTTest.subscribe(systemM)
        Thread.sleep(5000)
        //MqttSuccessObj.success = false
        MQTTTest.publish(TestConfig.request_jar)
        Thread.sleep(5000)

        eventually(timeout(60 seconds), interval(1 second)) {
          assert(MqttSuccessObj.success)
        }
      }

      "http jar" in {
        Http().bindAndHandle(route, MistConfig.HTTP.host, MistConfig.HTTP.port)
        val clientHTTP = Http(systemM)
        Thread.sleep(5000)
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
        Await.result(future_response, 60.seconds)
        eventually(timeout(60 seconds), interval(1 second)) {
          assert(http_response_success)
        }
      }

      "stopped" in {
        /*
        AddressAndSuccessForWorkerTest.success = false
        val workerManager = systemM.actorSelection(AddressAndSuccessForWorkerTest.serverAddress + AddressAndSuccessForWorkerTest.serverName)
        workerManager ! StopAllContexts
        Thread.sleep(5000)
        val workerTestActor = systemW.actorSelection(AddressAndSuccessForWorkerTest.nodeAddress + "/user/TestActor")
        val future = workerTestActor.ask(WorkerIsRemoved)(timeout = 1.day)
        var success = false
        future
          .onSuccess{
            case result:Boolean => { success = result }
            println("workerRemoved status", success)
          }
        Await.result(future, 30.seconds)
        workerManager ! ShutdownMaster
        Thread.sleep(5000)
        eventually(timeout(30 seconds), interval(1 second)) {
          assert(success)
        }
        */
      }



  }
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(Span(60, Seconds), Span(1, Second))
}

class MasterWorkerAppsTest extends WordSpecLike {
  "Must started" must { //TODO test started apps
    "Master App" in {
      Master.main(Array(""))
      Thread.sleep(20000)
    }
    "Worker App" in {
      Worker.main(Array("foo"))

      Thread.sleep(20000)
    }
  }
}