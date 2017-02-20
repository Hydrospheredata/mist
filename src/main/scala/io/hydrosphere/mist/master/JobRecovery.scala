package io.hydrosphere.mist.master

import akka.actor.Actor
import io.hydrosphere.mist.master.async.mqtt.{MQTTPubSub}
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobDetails}
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger
import org.json4s.jackson.Serialization

private[mist] class JobRecoveryActor extends Actor with Logger {

  override def receive: Receive = {
    case _: Any => println("received msg") 
  }
  
}

//case object StartRecovery
//case object JobCompleted
//case class TryRecoveryNext(collection: List[JobDetails])
//case class JobStarted(jobStartedCount: Int = 0)



// TODO: add abstract async interface instead of mqtt-specific one
//private[mist] class JobRecovery(configurationRepository: JobRepository) extends Actor with Logger {
//
//  private implicit val formats = org.json4s.DefaultFormats
//  private var startedJobCount = 0
//
//  override def receive: Receive = {
//
//    case StartRecovery =>
//      val allJobs = configurationRepository.getAll
//      logger.info(s"${allJobs.length} loaded from MapDb ")
//      configurationRepository.clear()
//      this.self ! TryRecoveryNext(allJobs)
//
//    case nextTry: TryRecoveryNext =>
//      if (startedJobCount < MistConfig.Recovery.recoveryMultilimit) {
//        if (nextTry.collection.nonEmpty) {
//          val jobDetails = nextTry.collection.last
//          val json = Serialization.write(jobDetails)
//          logger.info(s"send $json")
//          nextTry.collection.remove(TryRecoveryNext.collection.last)
//          pubsub ! new MQTTPubSub.Publish(json.getBytes("utf-8"))
//        }
//      }
//
//    case JobStarted =>
//      startedJobCount += 1
//      if (startedJobCount < MistConfig.Recovery.recoveryMultilimit) {
//        this.self ! TryRecoveryNext
//      }
//
//    case JobCompleted =>
//      startedJobCount -= 1
//      if (startedJobCount < MistConfig.Recovery.recoveryMultilimit) {
//        this.self ! TryRecoveryNext
//      }
//  }
//}
