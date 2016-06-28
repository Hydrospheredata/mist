package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.master.{JobCompleted, JobStarted}
import io.hydrosphere.mist.{Repository, Specification, MistConfig, Constants, Master}
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}


private[mist] trait ConfigurationRepository {
  def add(jobId: String, jobConfiguration: JobConfiguration): Unit = ???
  def remove(jobId: String): Unit = ???
  def get(jobId: String): JobConfiguration = ???
  def getAll: scala.collection.mutable.Map[String, JobConfiguration] = ???
  def size: Int = ???
  def clear(): Unit = ???
}

private[mist] object InMemoryJobConfigurationRepository extends ConfigurationRepository {

  private val _collection = scala.collection.mutable.Map[String, JobConfiguration]()

  override def add(jobId: String, jobConfiguration: JobConfiguration): Unit = {
    _collection put (jobId, jobConfiguration)
  }

  override def remove(jobId: String): Unit = {
    _collection - jobId
  }

  override def get(jobId: String): JobConfiguration = {
    _collection(jobId)
  }

  override def getAll: scala.collection.mutable.Map[String, JobConfiguration] = _collection

  override def size: Int = _collection.size

  override def clear(): Unit = {
    for(jobConf <- getAll){
      _collection - jobConf._1
    }
  }
}

private[mist] object InMapDbJobConfigurationRepository extends ConfigurationRepository {
  // Db
  private lazy val db  =  DBMaker
    .fileDB(MistConfig.Recovery.recoveryDbFileName)
    .fileLockDisable
    .closeOnJvmShutdown
    .make

  // Map
  private lazy val map = db
    .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
    .createOrOpen

  // Json formats
  private implicit val formats = org.json4s.DefaultFormats

  override def add(jobId: String, jobConfiguration: JobConfiguration): Unit = {
    try {
      val w_job = SerializationUtils.serialize(jobConfiguration)
      map.put(jobId, w_job)
      println(s"${jobId} saved in MapDb")
    } catch {
      case e: Exception => println(e)
    }
  }

  override def remove(jobId: String): Unit = {
    try {
      map.remove(jobId)
      println(s"${jobId} removed from MapDb")
    } catch{
      case e: Exception => println(e)
    }
  }

  override def getAll: scala.collection.mutable.Map[String, JobConfiguration] = {
    val _collection = scala.collection.mutable.Map[String, JobConfiguration]()
    try{
      val keys = map.getKeys.toArray()

      for(key <- keys){
        _collection put (key.toString, SerializationUtils.deserialize(map.get(key.toString)).asInstanceOf[JobConfiguration])
      }
      println(s"${_collection.size} loaded from MapDb")
    }
    catch {
      case e: Exception =>
        println(e)
    }
    _collection
  }

  override def get(jobId: String): JobConfiguration = {
    var jobConfiguration: JobConfiguration = ???
    try {
      jobConfiguration = SerializationUtils.deserialize(map.get(jobId)).asInstanceOf[JobConfiguration]
    }
    catch {
      case e: Exception =>
        println(e)
    }
    jobConfiguration
  }

  override def size: Int ={
    try{
      val keys = map.getKeys.toArray()
      var _count = 0
      for(key <- keys){
        _count += 1
      }
      _count
    }
    catch {
      case e: Exception =>
        println(e)
        0
    }
  }

 override def clear(): Unit = {
   try {
     map.clear()
   } catch {
     case e: Exception => println(e)
   }
 }
}


