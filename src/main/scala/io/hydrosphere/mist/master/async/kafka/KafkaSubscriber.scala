package io.hydrosphere.mist.master.async.kafka

import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.master.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.async.{AsyncInterface, AsyncSubscriber}
import io.hydrosphere.mist.utils.{Logger, MultiReceivable}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization

private[mist] object KafkaSubscriber {
  
  def props(publisherActor: ActorRef, actorWrapper: ActorRef): Props = Props(classOf[KafkaSubscriber], publisherActor, actorWrapper)
  
}

private[mist] class KafkaSubscriber(override val publisherActor: ActorRef, actorWrapper: ActorRef) extends AsyncSubscriber with MultiReceivable with Logger with JobConfigurationJsonSerialization {
  
  override val provider: Provider = AsyncInterface.Provider.Kafka

  override def preStart(): Unit = {
    super.preStart()
    
    actorWrapper ! KafkaActorWrapper.Subscribe(self)
  }
  
  override def postStop(): Unit = {
    super.postStop()
    
    actorWrapper ! KafkaActorWrapper.Unsubscribe
  }
  
  receiver {
    case KafkaActorWrapper.Message(_, value) => processIncomingMessage(value)
  }
}
