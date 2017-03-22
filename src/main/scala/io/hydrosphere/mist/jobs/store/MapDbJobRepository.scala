package io.hydrosphere.mist.jobs.store

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.utils.Logger
import org.mapdb.{DBMaker, Serializer}
import io.hydrosphere.mist.utils.json.JobDetailsJsonSerialization
import spray.json._


class MapDbJobRepository(filePath: String) extends JobRepository with JobDetailsJsonSerialization with Logger {
  // Db
  private lazy val db = DBMaker
    .fileDB(filePath)
    .fileLockDisable
    .closeOnJvmShutdown
    .checksumHeaderBypass()
    .make

  // Map
  private lazy val map = db
    .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
    .createOrOpen
  
  private def add(jobDetails: JobDetails): Unit = {
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

  private def getAll: List[JobDetails] = {
    try {
      val values = map.getKeys.toArray.toList.map { (key) =>
        logger.info(key.toString)
        new String(map.get(key.toString)).parseJson.convertTo[JobDetails]
      }
      logger.info(s"${values.length} get from MapDb")
      values
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

  override def size: Long ={
    try{
      val keys = map.getKeys.toArray()
      keys.length.toLong
    }
    catch {
      case e: Exception =>
        logger.error(e.getMessage, e)
        0L
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

  override def filteredByStatuses(statuses: List[Status]): List[JobDetails] = {
    getAll.filter {
      job: JobDetails => statuses contains job.status
    }
  }

  override def queuedInNamespace(namespace: String): List[JobDetails] = {
    getAll.filter {
      job => job.status == JobDetails.Status.Queued && job.configuration.namespace == namespace
    }
  }

  override def runningInNamespace(namespace: String): List[JobDetails] = {
    getAll.filter {
      job => job.status == JobDetails.Status.Running && job.configuration.namespace == namespace
    }
  }
}

object MapDbJobRepository extends MapDbJobRepository(MistConfig.History.filePath)
