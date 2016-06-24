package io.hydrosphere.mist.master


import akka.actor.{Actor, ActorRef, Props}
import io.hydrosphere.mist.master.MqttPubSub.Publish
import io.hydrosphere.mist.{MistConfig}
import io.hydrosphere.mist.jobs._
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer

case object StartRecovery

case object TryRecoveyNext{
 var _collection = ArrayBuffer.empty[JobConfiguration]
}
case object JobStarted{
  var jobStartedCount = 0
}
case object JobCompleted

private [mist] class JobRecovery(configurationRepository :ConfigurationRepository, jobRepository: JobRepository) extends Actor with MqttPubSubActor {

  private implicit val formats = org.json4s.DefaultFormats

  override def receive: Receive = {

    case StartRecovery =>{
      TryRecoveyNext._collection = configurationRepository.getAll
      configurationRepository.clear
      this.self ! TryRecoveyNext
    }

    case TryRecoveyNext =>{

        if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
          if (TryRecoveyNext._collection.size > 0) {
            val job_configuration = TryRecoveyNext._collection.last
            val json = Serialization.write(job_configuration)
            println(s"send ${json}")
            pubsub ! new Publish(json.getBytes("utf-8"))
            TryRecoveyNext._collection -= TryRecoveyNext._collection.last
          }
        }
    }

    case JobStarted =>{
      JobStarted.jobStartedCount += 1
      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        this.self ! TryRecoveyNext
      }
    }

    case JobCompleted => {
      JobStarted.jobStartedCount -= 1
      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        this.self ! TryRecoveyNext
      }
    }
  }
}