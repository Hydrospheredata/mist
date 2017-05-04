package io.hydrosphere.mist.master.interfaces.async2.mqtt

import io.hydrosphere.mist.master.interfaces.async2.AsyncClient
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import scala.concurrent.Future

class MqttClient(
  host: String,
  port: Int,
  publisherTopic: String,
  consumerTopic: String
) extends AsyncClient {

  private val client = {
    val persistence = new MemoryPersistence
    val uri = s"tcp://$host:$port"
    val client = new MqttAsyncClient(uri, MqttAsyncClient.generateClientId(), persistence)

    val opt = new MqttConnectOptions
    opt.setCleanSession(true)
    client.connect(opt)

    client
  }

  override def publish(event: String): Unit = {
    val message = new MqttMessage(event.getBytes())
    client.publish(publisherTopic, message)
  }

  override def subscribe(f: (String) => Unit): Unit = {
    client.setCallback(new MqttCallback {

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {}

      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        if (topic == consumerTopic) {
          val payload = message.getPayload
          val data = new String(payload)
          f(data)
        }
      }

      override def connectionLost(cause: Throwable): Unit = {}
    })
  }

  override def close(): Future[Unit] = {
    client.disconnect()
    Future.successful(())
  }
}
