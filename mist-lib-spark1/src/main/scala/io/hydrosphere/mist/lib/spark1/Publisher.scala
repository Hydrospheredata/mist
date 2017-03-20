package io.hydrosphere.mist.lib.spark1

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.eclipse.paho.client.mqttv3.{MqttMessage, MqttClient, MqttConnectOptions}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence


trait Publisher {

  def publish(bytes: Array[Byte]): Unit

  def publish(s: String): Unit = publish(s.getBytes)

  private [mist] def close(): Unit
}

object Publisher {

  import scala.collection.JavaConversions._

  val connectionStringR = "(kafka|mqtt)://(.*)".r
  /**
    *
    * @param connectionString
    *   kafka - kafka://bootstrap.servers
    *   mqtt -  mqtt://connectionUrl
    */
  def create(connectionString: String, sc: SparkContext): Publisher = {
    connectionStringR.findFirstMatchIn(connectionString) match {
      case Some(m) =>
        val groups = m.subgroups
        buildPublisher(groups.head, groups.last, sc)
      case None => throw new IllegalAccessException(s"Can not instancieate publisher for $connectionString")
    }
  }

  private def buildPublisher(
    protocol: String,
    connection: String,
    sc: SparkContext
  ): Publisher = {
    //TODO
    val topic = "test_topic"
    protocol match {
      case "kafka" =>
        val sink = KafkaSink(connection)
        val bc = sc.broadcast(sink)
        new KafkaPublisher(bc, topic)

      case "mqtt" =>
        val sink = MqttSink(connection)
        val bc = sc.broadcast(sink)
        new MqttPublisher(bc, topic)
    }
  }

  class KafkaPublisher(sink: Broadcast[KafkaSink], topic: String) extends Publisher {

    override def publish(bytes: Array[Byte]): Unit =
      sink.value.send(topic, bytes)

    override private[mist] def close(): Unit =
      sink.destroy()
  }

  class KafkaSink(create: () => KafkaProducer[String, Array[Byte]]) extends Serializable {

    lazy val producer = create()

    def send(topic: String, bytes: Array[Byte]): Unit = producer.send(new ProducerRecord(topic, bytes))
  }


  object KafkaSink {
    def apply(bootstrapServers: String): KafkaSink = {
      val f = () => {
        val config = Map[String, AnyRef](
          "bootstrap.servers" -> bootstrapServers,
          "retries" -> 0.underlying(),
          "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
          "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
        )
        val producer = new KafkaProducer[String, Array[Byte]](config)
        producer
      }
      new KafkaSink(f)
    }
  }

  class MqttPublisher(sink: Broadcast[MqttSink], topic: String) extends Publisher {

    override def publish(bytes: Array[Byte]): Unit =
      sink.value.send(topic, bytes)

    override private[mist] def close(): Unit = {
      sink.value.producer.disconnect()
      sink.destroy()
    }
  }

  class MqttSink(create: () => MqttClient) extends Serializable {

    lazy val producer = create()

    def send(topic: String, bytes: Array[Byte]): Unit =
      producer.publish(topic, new MqttMessage(bytes))
  }

  object MqttSink {

    def apply(connectionUrl: String): MqttSink = {
      val f = () => {
        val opt = new MqttConnectOptions
        opt.setCleanSession(true)

        val persistence = new MemoryPersistence
        val client = new MqttClient(connectionUrl, MqttClient.generateClientId(), persistence)
        client.connect()

        client
      }
      new MqttSink(f)
    }
  }
}
