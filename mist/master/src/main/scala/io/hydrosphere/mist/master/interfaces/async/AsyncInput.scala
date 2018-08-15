package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.master.interfaces.async.kafka.TopicConsumer
import io.hydrosphere.mist.utils.Logger
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence


trait AsyncInput {

  def start(f: String => Unit): Unit

  def close(): Unit

}

object AsyncInput {

  def forKafka(
    host: String,
    port: Int,
    topic: String
  ): AsyncInput = {
    new AsyncInput {

      val consumer = TopicConsumer(host, port, topic)

      override def start(f: (String) => Unit): Unit =
        consumer.subscribe({case (k, v) => f(v)})

      override def close(): Unit = consumer.close()
    }
  }

  def forMqtt(
    host: String,
    port: Int,
    topic: String
  ): AsyncInput = {
    new AsyncInput with Logger {

      val client = {
        val persistence = new MemoryPersistence
        val uri = s"tcp://$host:$port"
        val client = new MqttAsyncClient(uri, MqttAsyncClient.generateClientId(), persistence)

        val opt = new MqttConnectOptions
        opt.setCleanSession(true)
        opt.setAutomaticReconnect(true)
        client.connect(opt).waitForCompletion()
        client
      }

      override def start(f: (String) => Unit): Unit = {
        client.subscribe(topic, 0, new IMqttMessageListener {
          override def messageArrived(sourceTopic: String, message: MqttMessage): Unit = {
            if (sourceTopic == topic) {
              val data = new String(message.getPayload)
              f(data)
            }
          }
        })
      }

      override def close(): Unit = {
        try {
          client.disconnect()
        } catch {
          case _: Throwable =>
            client.disconnectForcibly()
        } finally {
          client.close()
        }
      }

    }

  }
}
