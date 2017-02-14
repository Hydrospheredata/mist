package io.hydrosphere.mist.master

import akka.actor.Actor
import io.hydrosphere.mist.master.mqtt.{MQTTPubSub, MQTTPubSubActor}
import io.hydrosphere.mist.jobs.{ConfigurationRepository, FullJobConfiguration}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger
import org.json4s.jackson.Serialization
import scala.collection.mutable

case object StartRecovery

case object TryRecoveryNext{
 var _collection: mutable.Map[String, FullJobConfiguration] = scala.collection.mutable.Map[String, FullJobConfiguration]()
}
case object JobStarted{
  var jobStartedCount = 0
}
case object JobCompleted

// TODO: add abstract async interface instead of mqtt-specific one
private[mist] class JobRecovery(configurationRepository :ConfigurationRepository) extends Actor with MQTTPubSubActor with Logger {

  private implicit val formats = org.json4s.DefaultFormats

  override def receive: Receive = {

    case StartRecovery =>
      TryRecoveryNext._collection = configurationRepository.getAll
      logger.info(s"${TryRecoveryNext._collection.size} loaded from MapDb ")
      configurationRepository.clear()
      this.self ! TryRecoveryNext

    case TryRecoveryNext =>

      if (JobStarted.jobStartedCount < MistConfig().Recovery.recoveryMultilimit) {
        if (TryRecoveryNext._collection.nonEmpty) {
          val job_configuration = TryRecoveryNext._collection.last
          val json = Serialization.write(job_configuration._2)
          logger.info(s"send $json")
          TryRecoveryNext._collection.remove(TryRecoveryNext._collection.last._1)
          pubsub ! new MQTTPubSub.Publish(json.getBytes("utf-8"))
        }
      }

    case JobStarted =>
      JobStarted.jobStartedCount += 1
      if (JobStarted.jobStartedCount < MistConfig().Recovery.recoveryMultilimit) {
        this.self ! TryRecoveryNext
      }

    case JobCompleted =>
      JobStarted.jobStartedCount -= 1
      if (JobStarted.jobStartedCount < MistConfig().Recovery.recoveryMultilimit) {
        this.self ! TryRecoveryNext
      }
  }
}
