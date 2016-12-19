package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.{Logger, MistConfig}
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}

private[mist] trait ConfigurationRepository extends Logger{
  def add(jobId: String, jobConfiguration: FullJobConfiguration): Unit
  def remove(jobId: String): Unit
  def get(jobId: String): FullJobConfiguration
  def getAll: scala.collection.mutable.Map[String, FullJobConfiguration]
  def size: Int
  def clear(): Unit
}

private[mist] object InMemoryJobConfigurationRepository extends ConfigurationRepository {

  private val _collection = scala.collection.mutable.Map[String, FullJobConfiguration]()

  override def add(jobId: String, jobConfiguration: FullJobConfiguration): Unit = {
    _collection put (jobId, jobConfiguration)
  }

  override def remove(jobId: String): Unit = {
    _collection.remove(jobId)
  }

  override def get(jobId: String): FullJobConfiguration = {
    _collection(jobId)
  }

  override def getAll: scala.collection.mutable.Map[String, FullJobConfiguration] = _collection

  override def size: Int = _collection.size

  override def clear(): Unit = {
    _collection.clear()
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

  override def add(jobId: String, jobConfiguration: FullJobConfiguration): Unit = {
    try {
      val w_job = SerializationUtils.serialize(jobConfiguration)
      map.put(jobId, w_job)
      logger.info(s"$jobId saved in MapDb")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  override def remove(jobId: String): Unit = {
    try {
      map.remove(jobId)
      logger.info(s"$jobId removed from MapDb")
    } catch{
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  override def getAll: scala.collection.mutable.Map[String, FullJobConfiguration] = {
    val _collection = scala.collection.mutable.Map[String, FullJobConfiguration]()
    try{
      val keys = map.getKeys.toArray()

      for(key <- keys){
        _collection put (key.toString, SerializationUtils.deserialize(map.get(key.toString)).asInstanceOf[FullJobConfiguration])
      }
      logger.info(s"${_collection.size} get from MapDb")
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
    }
    _collection
  }

  override def get(jobId: String): FullJobConfiguration = {
    try {
      SerializationUtils.deserialize(map.get(jobId)).asInstanceOf[FullJobConfiguration]
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        new FullJobConfiguration("", "", "")
    }
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
        logger.error(e.getMessage, e)
        0
    }
  }

 override def clear(): Unit = {
   try {
     map.clear()
     logger.info("MpDb cleaned", size)
   } catch {
     case e: Exception => logger.error(e.getMessage, e)
   }
 }
}


