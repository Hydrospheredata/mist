package io.hydrosphere.mist.api

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttConnectOptions, MqttMessage}


trait GlobalPublisher {

  def publish(bytes: Array[Byte]): Unit

  def publish(s: String): Unit = publish(s.getBytes)

  private [mist] def close(): Unit
}

object GlobalPublisher {

  import scala.collection.JavaConversions._

  val connectionStringR = "(kafka|mqtt)://(.*)".r

  //TODO: if there is no global publisher configuration??
  /**
    *
    * @param connectionString
    *   kafka - kafka://bootstrap.servers
    *   mqtt -  mqtt://connectionUrl
    */
  def create(connectionString: String, topic: String, sc: SparkContext): GlobalPublisher = {
    connectionStringR.findFirstMatchIn(connectionString) match {
      case Some(m) =>
        val groups = m.subgroups
        buildPublisher(groups.head, groups.last, topic, sc)
      case None =>
        throw new IllegalAccessException(s"Can not instantiate publisher for $connectionString")
    }
  }

  private def buildPublisher(
    protocol: String,
    connection: String,
    topic: String,
    sc: SparkContext
  ): GlobalPublisher = {
    val sink = protocol match {
      case "kafka" =>
        KafkaSink(connection)

      case "mqtt" =>
        MqttSink(connection)
    }
    val bc = sc.broadcast(sink)
    new BcPublisher(bc, topic)
  }

  trait Sink extends Serializable {

    def send(topic: String, bytes: Array[Byte]): Unit

    def close(): Unit
  }

  class KafkaSink(create: () => KafkaProducer[String, Array[Byte]]) extends Sink {

    lazy val producer = create()

    def send(topic: String, bytes: Array[Byte]): Unit = producer.send(new ProducerRecord(topic, bytes))

    override def close(): Unit = producer.close()
  }


  class MqttSink(create: () => MqttClient) extends Sink {

    lazy val producer = create()

    def send(topic: String, bytes: Array[Byte]): Unit =
      producer.publish(topic, new MqttMessage(bytes))

    override def close(): Unit = {
      producer.disconnect()
      producer.close()
    }
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

  class BcPublisher(sink: Broadcast[Sink], topic: String) extends GlobalPublisher with Serializable {

    override def publish(bytes: Array[Byte]): Unit = sink.value.send(topic, bytes)

    override private[mist] def close(): Unit = {
      sink.value.close()
      sink.destroy()
    }
  }
}
