package io.hydrosphere.mist.master


import akka.actor.Actor
import io.hydrosphere.mist.master.mqtt.{MqttPubSubActor, MqttPubSub}
import MqttPubSub.Publish
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.{JobConfiguration, ConfigurationRepository, JobRepository}
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer

case object StartRecovery

case object TryRecoveyNext{
 var _collection = scala.collection.mutable.Map[String, JobConfiguration]()
}
case object JobStarted{
  var jobStartedCount = 0
}
case object JobCompleted

private[mist] class JobRecovery(configurationRepository :ConfigurationRepository) extends Actor with MqttPubSubActor {

  private implicit val formats = org.json4s.DefaultFormats

  override def receive: Receive = {

    case StartRecovery =>
      TryRecoveyNext._collection = configurationRepository.getAll
      configurationRepository.clear()
      this.self ! TryRecoveyNext

    case TryRecoveyNext =>

      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        if (TryRecoveyNext._collection.nonEmpty) {
          val job_configuration = TryRecoveyNext._collection.last
          val json = Serialization.write(job_configuration._2)
          println(s"send $json")
          pubsub ! new Publish(json.getBytes("utf-8"))
          TryRecoveyNext._collection - TryRecoveyNext._collection.last._1
        }
      }

    case JobStarted =>
      JobStarted.jobStartedCount += 1
      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        this.self ! TryRecoveyNext
      }

    case JobCompleted =>
      JobStarted.jobStartedCount -= 1
      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        this.self ! TryRecoveyNext
      }
  }
}
