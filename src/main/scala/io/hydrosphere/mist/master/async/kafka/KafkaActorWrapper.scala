package io.hydrosphere.mist.master.async.kafka

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorRef, Props, Terminated}
import io.hydrosphere.mist.utils.Logger
import java.util.{Collections, Properties}

import io.hydrosphere.mist.{Constants, MistConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

private[mist] object KafkaActorWrapper {
  
  case class Message(key: String, value: String)
  case class Subscribe(actor: ActorRef)
  case class SubscribeAck(subscribe: Subscribe)
  case object Unsubscribe

  class Subscribers extends Actor {
    private var subscribers = Set.empty[ActorRef]

    def receive: Receive = {
      case msg: Message =>
        subscribers foreach (_ ! msg)

      case msg@Subscribe(ref) =>
        context watch ref
        subscribers += ref
        ref ! SubscribeAck(msg)

      case Terminated(ref) =>
        subscribers -= ref
        if (subscribers.isEmpty) context stop self
    }
  }
  
  def props(): Props = Props(classOf[KafkaActorWrapper])
  
}

private[mist] class KafkaActorWrapper extends Actor with Logger {
  
  private val consumer = {
    val props = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${MistConfig.Kafka.host}:${MistConfig.Kafka.port}")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, MistConfig.Kafka.groupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props
    }
    new KafkaConsumer[String, String](props)
  }

  override def receive: Receive = {
    case msg@KafkaActorWrapper.Subscribe(_) =>
      context.child(Constants.Actors.kafkaServiceName) match {
        case Some(t) => t ! msg
        case None =>
          val t = context.actorOf(Props[KafkaActorWrapper.Subscribers], name = Constants.Actors.kafkaServiceName)
          t ! msg
          context watch t

          logger.debug(s"Subscribing to kafka topic: ${MistConfig.Kafka.subscribeTopic}")
          consumer.subscribe(Collections.singletonList(MistConfig.Kafka.subscribeTopic))
      }

      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          logger.debug(s"Start polling kafka topic ${MistConfig.Kafka.subscribeTopic}")
          while (true) {
            val records = consumer.poll(Int.MaxValue).asScala.toList
            records.foreach {
              record =>
                logger.debug(s"Received message: (${record.key()}, ${record.value()} at offset ${record.offset()})")
                context.self ! KafkaActorWrapper.Message(record.key(), record.value())
            }
          }
        }
      })
      
      context become ready
  }
  
  def ready: Receive = {
    case msg: KafkaActorWrapper.Message => context.child(Constants.Actors.kafkaServiceName) foreach (_ ! msg)

    case KafkaActorWrapper.Unsubscribe => consumer.close()
  }
}
