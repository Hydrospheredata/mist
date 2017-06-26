package io.hydrosphere.mist.master

import io.hydrosphere.mist.Messages.StatusMessages.SystemEvent
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.interfaces.async.kafka.TopicProducer
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttAsyncClient, MqttConnectOptions, MqttMessage}

trait JobEventPublisher {

  def notify(event: SystemEvent): Unit

  def close(): Unit

}

object JobEventPublisher {

  import JsonCodecs._
  import spray.json._

  def forKafka(host: String, port: Int, topic: String): JobEventPublisher = {
    new JobEventPublisher {
      val producer = TopicProducer(host, port, topic)

      override def notify(event: SystemEvent): Unit = {
        val json = event.toJson
        producer.send("", json)
      }

      override def close(): Unit = {}
    }
  }

  def forMqtt(host: String, port: Int, topic: String): JobEventPublisher = {
    new JobEventPublisher {

      val client = {
        val persistence = new MemoryPersistence
        val uri = s"tcp://$host:$port"
        val client = new MqttAsyncClient(uri, MqttAsyncClient.generateClientId(), persistence)

        val opt = new MqttConnectOptions
        opt.setCleanSession(true)
        client.connect(opt)

        client
      }

      override def notify(event: SystemEvent): Unit = {
        val json = event.toJson
        val message = new MqttMessage(json.getBytes())
        client.publish(topic, message)
      }

      override def close(): Unit = client.close()
    }
  }

}
