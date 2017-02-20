package io.hydrosphere.mist.jobs.store

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.utils.Logger
import org.apache.commons.lang.SerializationUtils
import org.mapdb.{DBMaker, Serializer}
import io.hydrosphere.mist.utils.json.{JobConfigurationJsonSerialization, JobDetailsJsonSerialization}
import spray.json._


private[mist] object MapDbJobRepository extends JobRepository with JobDetailsJsonSerialization with Logger {
  // Db
  private lazy val db = DBMaker
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

  override def add(jobDetails: JobDetails): Unit = {
    try {
      val w_job = jobDetails.toJson.compactPrint.getBytes
      map.put(jobDetails.jobId, w_job)
      logger.info(s"${jobDetails.jobId} saved in MapDb")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  override def remove(jobId: String): Unit = {
    try {
      map.remove(jobId)
      logger.info(s"$jobId removed from MapDb")
    } catch {
      case e: Exception => logger.error(e.getMessage, e)
    }
  }

  override def getAll: List[JobDetails] = {
    try {
      val keys = map.getKeys.toArray.toList.map { (key) =>
        new String(map.get(key.toString)).parseJson.convertTo[JobDetails]
      }
      logger.info(s"${keys.length} get from MapDb")
      keys
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        List.empty[JobDetails]
    }
  }

  override def get(jobId: String): Option[JobDetails] = {
    try {
      Some(new String(map.get(jobId)).parseJson.convertTo[JobDetails])
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        None
    }
  }

  override def size: Int ={
    try{
      val keys = map.getKeys.toArray()
      keys.length
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

  override def update(jobDetails: JobDetails): Unit = {
    add(jobDetails)
  }
}
