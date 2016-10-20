package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKit
import io.hydrosphere.mist.master.{JsonFormatSupport}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import spray.json.DefaultJsonProtocol
import io.hydrosphere.mist.master.mqtt.{MQTTServiceActor, MqttSubscribe}
import io.hydrosphere.mist.jobs.{InMemoryJobConfigurationRepository}
import io.hydrosphere.mist.worker.{ContextNode, JobRunnerNode}
import org.eclipse.paho.client.mqttv3.{IMqttDeliveryToken, MqttCallback, MqttClient, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import scala.concurrent.duration._


class InfinityJobTestActor extends WordSpecLike with Eventually with BeforeAndAfterAll with ScalaFutures with Matchers with JsonFormatSupport with DefaultJsonProtocol {

  val systemW = ActorSystem("mist", MistConfig.Akka.Worker.settings)
  val systemS = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val mqttActor = systemW.actorOf(Props(classOf[MQTTServiceActor]))
  mqttActor ! MqttSubscribe
  InfinityJobTestMqttActor.subscribe(systemW)

  override def beforeAll() = {
    Thread.sleep(5000)

  }

  override def afterAll() = {
    InfinityJobTestMqttActor.disconnect

    TestKit.shutdownActorSystem(systemW)
    TestKit.shutdownActorSystem(systemS)
    Thread.sleep(5000)
  }

  "Streaming" must {
    "Start and success" in {

      systemW.actorOf(ContextNode.props("streaming"), name = "streaming")
      Thread.sleep(5000)

      MqttSuccessObj.success = false
      systemS.actorOf(Props(new JobRunnerNode(TestConfig.examplesPath, "SimpleSparkStreaming$", "streaming", "123456789", Map().empty)), name = "JobStarter")

      eventually(timeout(30 seconds), interval(1 second)) {
        assert(MqttSuccessObj.success)
      }
    }
  }
}

object InfinityJobTestMqttActor{

  val persistence = new MemoryPersistence
  val mqttClient = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)

  def subscribe(actorSystem: ActorSystem): Unit = {
    mqttClient.connect()
    mqttClient.subscribe(MistConfig.MQTT.subscribeTopic)

    val callback = new MqttCallback {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        println("Receiving Data Test, Topic : %s, Message : %s".format(topic, message))
        val stringMessage = message.toString

        if( stringMessage contains "test message from stream job" )
          MqttSuccessObj.success = true
      }

      override def connectionLost(cause: Throwable): Unit = {
        println(cause)
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }
    }
    mqttClient.setCallback(callback)
  }

  def disconnect: Unit = {
    mqttClient.disconnect()
  }
}