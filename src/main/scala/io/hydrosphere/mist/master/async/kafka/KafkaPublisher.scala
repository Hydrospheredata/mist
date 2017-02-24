package io.hydrosphere.mist.master.async.kafka

import akka.actor.Props
import cakesolutions.kafka.KafkaProducer
import cakesolutions.kafka.akka.{KafkaProducerActor, ProducerRecords}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncPublisher
import io.hydrosphere.mist.utils.Logger
import org.apache.kafka.common.serialization.StringSerializer

object KafkaPublisher {
  
  def props(): Props = Props(classOf[KafkaPublisher])
  
}

private[mist] class KafkaPublisher extends AsyncPublisher with Logger {

  override def send(message: String): Unit = {
    val kafkaProducer = context.actorOf(KafkaProducerActor.props(
      KafkaProducer.Conf(
        props = Map("bootstrap.servers" -> s"${MistConfig.Kafka.host}:${MistConfig.Kafka.port}"),
        keySerializer = new StringSerializer,
        valueSerializer = new StringSerializer
      ).withConf(MistConfig.Kafka.conf)
    ))
    kafkaProducer ! ProducerRecords.fromKeyValues(MistConfig.Kafka.publishTopic, Seq((Some("result"), message)), None, None)
  }
  
}
