package io.hydrosphere.mist

import java.util.concurrent.atomic.AtomicBoolean

import com.dimafeng.testcontainers.{Container, GenericContainer}
import io.hydrosphere.mist.master.Messages.Status._
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttMessageListener, MqttClient, MqttMessage}
import org.junit.runner.Description
import org.scalatest.FunSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minute, Seconds, Span}
import org.testcontainers.containers.wait.Wait

class MqttInterfaceTest extends FunSpec with MistItTest with Eventually {

  import JsonCodecs._
  import spray.json.pimpString

  implicit private val suiteDescription = Description.createSuiteDescription(getClass)
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))
  override val overrideConf = Some("mqtt/integration.conf")

  var container: Container = _
  var mqttClient: MqttClient = _

  override def beforeAll = {
    container = GenericContainer(
      "ansi/mosquitto:latest", Map(1883 -> 1883),
      waitStrategy = Wait.forListeningPort()
    )
    container.starting()

    super.beforeAll()

    val persistence = new MemoryPersistence
    mqttClient = new MqttClient(s"tcp://localhost:1883", MqttClient.generateClientId, persistence)
    mqttClient.connect()
  }


  override def afterAll = {
    try {
      mqttClient.disconnect()
    } finally {
      mqttClient.close()
    }
    container.finished()
    super.afterAll
  }

  it("should run job by mqtt") {
    val request =
      """
        |{
        |  "endpointId": "simple-context-py",
        |  "parameters": {
        |    "numbers": [1, 2, 3]
        |  }
        |}
      """.stripMargin
    val message = new MqttMessage(request.getBytes)
    mqttClient.publish("foo", message)

    val resultReceived = new AtomicBoolean(false)

    mqttClient.subscribe("foo", new IMqttMessageListener {
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

    eventually(timeout(Span(1, Minute))) {
      assert(resultReceived.get)
    }

  }

}
