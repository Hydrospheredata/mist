package io.hydrosphere.mist.actors

import akka.actor.{Props, ActorSystem, Actor}

import io.hydrosphere.mist.{MistConfig}
import io.hydrosphere.mist.jobs.{JobByIdSpecification, InMemoryJobRepository, JobConfiguration, Job}
import org.apache.commons.lang.SerializationUtils
import org.json4s.jackson.Serialization
import org.mapdb.{Serializer, DBMaker}

case object TryRecoveyNext
case object JobStarted{
  var jobStartedCount = 0
}
case object JobComplited

private [mist] object InMapDbJobConfigurationRepository{

  // Job Recovery Actor
  private lazy val recoveryActor =  ActorSystem("mist").actorOf(Props[JobRecovery], name = "recoveryActor")

  // Db
  private lazy val db  =  DBMaker
    .fileDB(MistConfig.MQTT.recoveryDbFileName)
    .fileLockDisable
    .closeOnJvmShutdown
    .make

  // Map
  private lazy val map = db
    .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
    .createOrOpen

  // Json formats
  private implicit val formats = org.json4s.DefaultFormats

  def getKeys: Array[AnyRef] ={
    try {
      map.getKeys.toArray()
    } catch {
      case e: Exception => {
        println(e)
        Array.empty
      }
    }
  }

  def addJobConfigurationByJob(job: Job) {
    try {
      val w_job = SerializationUtils.serialize(job.configuration)
      map.put(job.id, w_job)
      println(s"${job.id} saved in db")
      recoveryActor ! JobStarted
    } catch {
      case e: Exception => println(e)
    }
  }

  def getJobConfiguration(job_id: String):JobConfiguration = {
      SerializationUtils.deserialize(map.get(job_id)).asInstanceOf[JobConfiguration]
  }

  def getLastId: String = {
    try{
      if(map.size() > 0)
       {
         getKeys.last.toString
       }
      else{
        return "None"
      }
    } catch {
      case e: Exception => {
        println(e)
        "None"
      }
    }
  }

  def printStatus =
  {
    try {
      println(s"Opened recove DB: ${db} ")
      println(s"Jobs for recovery: ${map.size()}")
    } catch{
      case e: Exception => println(e)
    }
  }

  def jobComplit(job: Job) = {
    try {
      map.remove(job.id)
      println(s"${job.id} removed from db")
      recoveryActor ! JobComplited
    } catch{
      case e: Exception => println(e)
    }
  }

  def removeJobById(job_id: String) = {
    try {
      map.remove(job_id)
      println(s"${job_id} removed from db")
    } catch {
      case e: Exception => println(e)
    }
  }

  def runRecovery = {
    if(MistConfig.MQTT.recoveryOn)
    try {
      if(map.size() > 0)
        recoveryActor ! TryRecoveyNext
    } catch {
      case e: Exception => None
    }
  }
}

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
