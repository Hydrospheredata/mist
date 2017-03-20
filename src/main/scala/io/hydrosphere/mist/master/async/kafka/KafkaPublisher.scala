package io.hydrosphere.mist.master.async.kafka

import java.util.Properties

import akka.actor.Props
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncPublisher
import io.hydrosphere.mist.utils.Logger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

private[mist] object KafkaPublisher {
  
  def props(): Props = Props(classOf[KafkaPublisher])
  
}

private[mist] class KafkaPublisher extends AsyncPublisher with Logger {
  
  private val producer = {
    val props = new Properties()
    props.put("bootstrap.servers", s"${MistConfig.Kafka.host}:${MistConfig.Kafka.port}")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    new KafkaProducer[String, String](props)
  }
  
  override def send(message: String): Unit = {
    producer.send(new ProducerRecord[String, String](MistConfig.Kafka.publishTopic, "", message))
  }

  override def postStop(): Unit = {
    super.postStop()
    
    producer.close()
  }
}
