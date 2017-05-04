package io.hydrosphere.mist.master.interfaces.async2.kafka

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import io.hydrosphere.mist.master.interfaces.async2.AsyncIO
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.concurrent.{Promise, Future}

class TopicProducer[K, V](
  producer: KafkaProducer[K, V],
  topic: String
) {

  def send(key:K, value: V): Unit = {
    val record = new ProducerRecord(topic, key, value)
    producer.send(record)
  }

}

class TopicConsumer[K, V](
  consumer: KafkaConsumer[K, V],
  topic: String,
  timeout: Long = 100
) {

  private val promise = Promise[Unit]
  private val stopped = new AtomicBoolean(false)

  def subscribe(f: (K, V) => Unit): Future[Unit] = {
    run(f)
    promise.future
  }

  private def run(f: (K, V) => Unit): Unit = {
    consumer.subscribe(Seq(topic).asJava)
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        while (!stopped.get()) {
          val records = consumer.poll(timeout).asScala
          records.foreach(r => f(r.key(), r.value()))
        }
        promise.success(())
      }
    })
    thread.setName(s"kafka-topic-consumer-$topic")
    thread.start()
  }

  def close(): Future[Unit] = {
    stopped.set(true)
    promise.future
  }
}

case class KafkaClient(
  producer: TopicProducer[String, String],
  consumer: TopicConsumer[String, String]
) extends AsyncIO {

  override def publish(event: String): Unit = producer.send("", event)

  override def subscribe(f: (String) => Unit): Unit =
    consumer.subscribe { case (k, v) => f(v) }

  override def close(): Future[Unit] = consumer.close()
}

object KafkaClient {

  def create(
    host: String,
    port: Int,
    publisherTopic: String,
    consumerTopic: String
  ): KafkaIO = KafkaIO(
      mkProducer(host, port, publisherTopic),
      mkConsumer(host, port, consumerTopic)
    )


  private def mkProducer(
    host: String,
    port: Int,
    topic: String): TopicProducer[String, String] = {

    val props = new java.util.Properties()
    props.put("bootstrap.servers", s"$host:port")

    val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)
    new TopicProducer(producer, topic)
  }

  private def mkConsumer(
    host: String,
    port: Int,
    topic: String
  ): TopicConsumer[String, String] = {
    val props = new java.util.Properties()
    props.put("bootstrap.servers", s"$host:$port")
    props.put("group.id", "mist-" + UUID.randomUUID().toString)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
    new TopicConsumer(consumer, topic)
  }
}
