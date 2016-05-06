package io.hydrosphere.mist.actors

import akka.actor.Actor

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
case object JobComplited


private [mist] class JobRecovery(configurationRepository :ConfigurationRepository, jobRepository: JobRepository) extends Actor {

  private implicit val formats = org.json4s.DefaultFormats

  override def receive: Receive = {

    case StartRecovery =>{
      TryRecoveyNext._collection = configurationRepository.getAll
      this.self ! TryRecoveyNext
    }

    case TryRecoveyNext =>{

        if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
          if(TryRecoveyNext._collection.size > 0 ) {
            val job_configuration = TryRecoveyNext._collection.last
            val json = Serialization.write(job_configuration)
            MQTTService.publish(json)
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

    case JobComplited => {
      JobStarted.jobStartedCount -= 1
      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        this.self ! TryRecoveyNext
      }
    }
  }
}