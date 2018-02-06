package io.hydrosphere.mist

import java.util.concurrent.atomic.AtomicBoolean

import com.dimafeng.testcontainers.{Container, GenericContainer}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.master.Messages.StatusMessages.{FinishedEvent, SystemEvent}
import io.hydrosphere.mist.master.{MasterConfig, MasterServer, ServerInstance}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttMessageListener, MqttClient, MqttMessage}
import org.junit.runner.Description
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minute, Minutes, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import org.testcontainers.containers.wait.Wait

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExamplesOnLocalProcessSpec
  extends FunSpec
  with BeforeAndAfterAll
  with Matchers
  with Eventually {

  implicit private val suiteDescription = Description.createSuiteDescription(getClass)
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))

  var instance: ServerInstance = _
  var container: Container = _
  var mqttClient: MqttClient = _

  val interface = MistHttpInterface("localhost", 2004)

  private def startMist: ServerInstance = {
    val mistHome = sys.props.get("mistHome").get
    val cfgStr =
      s"""
         |mist {
         |  mqtt {
         |    on = true
         |    host = "localhost"
         |    port = 1883
         |    subscribe-topic = "in"
         |    publish-topic = "out"
         |  }
         |  work-directory = "${mistHome}"
         |}
      """.stripMargin
    val config = MasterConfig.parse("", MasterConfig.resolveUserConf(ConfigFactory.parseString(cfgStr)))
    val starting = MasterServer.start(config, "")
    Await.result(starting, Duration.Inf)
  }

  override def beforeAll = {
    container = GenericContainer(
      "ansi/mosquitto:latest", Map(1883 -> 1883),
      waitStrategy = Wait.forListeningPort()
    )
    container.starting()
    instance = startMist
    val persistence = new MemoryPersistence
    mqttClient = new MqttClient(s"tcp://localhost:1883", MqttClient.generateClientId, persistence)
    mqttClient.connect()
  }

  override def afterAll = {
    Await.result(instance.stop(), Duration.Inf)
    container.finished()
  }


  it("spark-ctx-example") {
    Thread.sleep(1000 * 10 * 2)
    val result = interface.runJob("spark-ctx-example",
      "numbers" -> List(1, 2, 3),
      "multiplier" -> 2
    )

    result.success shouldBe true
  }

  it("should run py simple context") {
    val result = interface.runJob("simple_context",
      "numbers" -> List(1, 2, 3)
    )
    assert(result.success, s"Job is failed $result")
  }

  it("should run session py") {
    val result = interface.runJob("session_job",
      "path" -> "./mist-tests/resources/hive_job_data.json"
    )
    assert(result.success, s"Job is failed $result")
  }

  it("should run job by mqtt") {
    import io.hydrosphere.mist.master.interfaces.JsonCodecs._
    import spray.json.enrichString
    val request =
      """
        |{
        |  "endpointId": "simple_context",
        |  "parameters": {
        |    "numbers": [1, 2, 3]
        |  }
        |}
      """.stripMargin
    val message = new MqttMessage(request.getBytes)
    mqttClient.publish("in", message)

    val resultReceived = new AtomicBoolean(false)

    mqttClient.subscribe("out", new IMqttMessageListener {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        val data = new String(message.getPayload)
        try {
          val result = data.parseJson.convertTo[SystemEvent]
          result match {
            case x: FinishedEvent =>
              resultReceived.set(true)
            case _ =>
          }
        } catch {
          case e: Throwable =>
        }
      }
    })

    eventually(timeout(Span(2, Minutes))) {
      assert(resultReceived.get)
    }

  }
}
