package io.hydrosphere.mist.master.interfaces.async.kafka

import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.interfaces.async.{AsyncInterface, AsyncSubscriber}
import io.hydrosphere.mist.utils.{Logger, MultiReceivable}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization

object KafkaSubscriber {
  
  def props(
    publisherActor: ActorRef,
    actorWrapper: ActorRef,
    masterService: MasterService): Props = {

    Props(classOf[KafkaSubscriber], publisherActor, actorWrapper, masterService)
  }
  
}

class KafkaSubscriber(
  override val publisherActor: ActorRef,
  actorWrapper: ActorRef,
  masterService: MasterService)
  extends AsyncSubscriber(masterService)
  with MultiReceivable with Logger with JobConfigurationJsonSerialization {
  
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
