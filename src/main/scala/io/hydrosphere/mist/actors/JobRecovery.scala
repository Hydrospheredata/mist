package io.hydrosphere.mist.actors

import akka.actor.Actor

import io.hydrosphere.mist.{MistConfig}
import io.hydrosphere.mist.jobs._
import org.json4s.jackson.Serialization

case object TryRecoveyNext
case object JobStarted{
  var jobStartedCount = 0
}
case object JobComplited

private [mist] class JobRecovery extends Actor {

 private implicit val formats = org.json4s.DefaultFormats

  override def receive: Receive = {

     case TryRecoveyNext =>{
       if(!InMapDbJobConfigurationRepository.getKeys.isEmpty)
         if (JobStarted.jobStartedCount < MistConfig.MQTT.recoveryMultilimit) {
           val job_id = InMapDbJobConfigurationRepository.getLastId
           if(job_id != "None")
           if (InMemoryJobRepository.get(new JobByIdSpecification(job_id)).isEmpty) {
             println(s"Recover job ${job_id}")
             val json = Serialization.write(InMapDbJobConfigurationRepository.getJobConfiguration(job_id))
             MQTTService.publish(json)
             InMapDbJobConfigurationRepository.removeJobById(job_id)
             JobStarted.jobStartedCount +=1
           }
         }
     }

     case JobStarted =>{
       if (JobStarted.jobStartedCount < MistConfig.MQTT.recoveryMultilimit) {
         this.self ! TryRecoveyNext
       }
     }

     case JobComplited => {
       JobStarted.jobStartedCount -= 1
       if (JobStarted.jobStartedCount < MistConfig.MQTT.recoveryMultilimit) {
         this.self ! TryRecoveyNext
       }
     }
   }
}
