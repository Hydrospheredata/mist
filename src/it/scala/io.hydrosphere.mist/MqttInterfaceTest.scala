package io.hydrosphere.mist

import java.util.concurrent.atomic.AtomicBoolean

import com.dimafeng.testcontainers.{Container, GenericContainer}
import io.hydrosphere.mist.jobs.JobResult
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{IMqttMessageListener, MqttClient, MqttMessage}
import org.junit.runner.Description
import org.scalatest.FunSpec
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.testcontainers.containers.wait.Wait

class MqttInterfaceTest extends FunSpec with MistItTest with Eventually {

  import io.hydrosphere.mist.master.interfaces.http.JsonCodecs._
  import spray.json.pimpString

  override val overrideConf = Some("mqtt/integration.conf")

  it("should run job by mqtt") {
    val request =
      """
        |{
        |  "jobId": "simple-context-py",
        |  "action": "execute",
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
        val result = data.parseJson.convertTo[JobResult]
        if (result.success) resultReceived.set(true)
      }
    })

    eventually(timeout(Span(30, Seconds))) {
      assert(resultReceived.get)
    }

  }

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

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))

  implicit private val suiteDescription = Description.createSuiteDescription(getClass)

  override def afterAll = {
    super.afterAll

    mqttClient.disconnect()
    mqttClient.close()
    container.finished()
  }
}
