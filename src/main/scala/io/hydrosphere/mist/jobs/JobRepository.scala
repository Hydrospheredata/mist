package io.hydrosphere.mist.jobs

import akka.actor.{Props, ActorSystem}
import io.hydrosphere.mist.actors.{TryRecoveyNext, JobComplited, JobRecovery, JobStarted}
import io.hydrosphere.mist.{MistConfig, Specification, Repository}
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{Serializer, DBMaker}

import scala.collection.mutable.ArrayBuffer

private[mist] trait JobRepository extends Repository[Job]

private[mist] object InMemoryJobRepository extends JobRepository {

  private val _collection = ArrayBuffer.empty[Job]

  override def add(job: Job): Unit = {
    _collection += job
  }

  override def get(specification: Specification[Job]): Option[Job] = {
    val predicate: Job => Boolean = x => specification.specified(x)
    _collection.find(predicate)
  }

  override def filter(specification: Specification[Job]): List[Job] = {
    val predicate: Job => Boolean = x => specification.specified(x)
    _collection.filter(predicate).toList
  }

  override def remove(job: Job): Unit = {
    _collection -= job
  }
}

private [mist] object InMapDbJobConfigurationRepository {

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