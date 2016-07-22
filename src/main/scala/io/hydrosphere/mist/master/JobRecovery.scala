package io.hydrosphere.mist.master


import akka.actor.Actor
import io.hydrosphere.mist.master.mqtt.{MqttPubSub, MqttPubSubActor}
import MqttPubSub.Publish
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.{ConfigurationRepository, JobConfiguration}
import org.json4s.jackson.Serialization

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
      println(s"${TryRecoveyNext._collection.size} loaded from MapDb ")
      configurationRepository.clear()
      this.self ! TryRecoveyNext

    case TryRecoveyNext =>

      if (JobStarted.jobStartedCount < MistConfig.Recovery.recoveryMultilimit) {
        if (TryRecoveyNext._collection.nonEmpty) {
          val job_configuration = TryRecoveyNext._collection.last
          val json = Serialization.write(job_configuration._2)
          println(s"send $json")
          TryRecoveyNext._collection.remove(TryRecoveyNext._collection.last._1)
          pubsub ! new Publish(json.getBytes("utf-8"))
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
