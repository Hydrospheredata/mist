package io.hydrosphere.mist.master.async.kafka

import akka.actor.{Actor, Props}
import cakesolutions.kafka.{KafkaConsumer, KafkaProducer}
import cakesolutions.kafka.akka.{ConsumerRecords, KafkaConsumerActor, KafkaProducerActor, ProducerRecords}
import cakesolutions.kafka.akka.KafkaConsumerActor.{Confirm, Subscribe, Unsubscribe}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncServiceActor
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization

import scala.concurrent.duration._

object KafkaSubscriberActor {

  def props() = Props(classOf[KafkaSubscriberActor])

}

class KafkaSubscriberActor extends AsyncServiceActor with Logger with JobConfigurationJsonSerialization {

  private val extractor = ConsumerRecords.extractor[String, String]

  private val kafkaConsumer = context.actorOf(KafkaConsumerActor.props(
    consumerConf = KafkaConsumer.Conf( 
      keyDeserializer = new StringDeserializer,
      valueDeserializer = new StringDeserializer,
      props = Map("bootstrap.servers" -> s"${MistConfig.Kafka.host}:${MistConfig.Kafka.port}")
    ).withConf(MistConfig.Kafka.conf),
    actorConf = KafkaConsumerActor.Conf(scheduleInterval = 1.seconds, unconfirmedTimeout = 3.seconds, maxRedeliveries = 3),
    self
  ), "KafkaConsumer")
  context.watch(kafkaConsumer)

  private val kafkaProducer = context.actorOf(KafkaProducerActor.props(
    KafkaProducer.Conf(
      props = Map("bootstrap.servers" -> s"${MistConfig.Kafka.host}:${MistConfig.Kafka.port}"), 
      keySerializer = new StringSerializer, 
      valueSerializer = new StringSerializer
    ).withConf(MistConfig.Kafka.conf)
  ))


  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"Subscribe to kafka topic: ${MistConfig.Kafka.subscribeTopic}")
    kafkaConsumer ! Subscribe.AutoPartition(List(MistConfig.Kafka.subscribeTopic))
  }

  override def postStop(): Unit = {
    logger.info(s"Unsubscribe from kafka topic ${MistConfig.Kafka.subscribeTopic}")
    kafkaConsumer ! Unsubscribe
    super.postStop()
  }

  override def receive: Receive = {
    case extractor(consumerRecords) =>
      consumerRecords.pairs.foreach {
        case (key, message) =>
          key match {
            case Some(k) if k == "error" => logger.debug("Received error, skip")
            case _ =>
              logger.info("Receiving Data from Kafka, Topic : %s, Message : %s".format(MistConfig.Kafka.subscribeTopic, message))
              processIncomingMessage(message)
          }
      }

      kafkaConsumer ! Confirm(consumerRecords.offsets, commit = true)
  }

  override def send(message: String): Unit = {
    kafkaProducer ! ProducerRecords.fromValues(MistConfig.Kafka.publishTopic, List(message), None, None)
  }
}
