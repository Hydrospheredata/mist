package io.hydrosphere.mist.cli

import io.hydrosphere.mist.jobs.JobDetails
import org.joda.time.DateTime

private[mist] trait Description {
  def prettyPrint: String = {
    fields.mkString("\t")
  }

  def fieldSizes: List[Int] = {
    fields.map(_.length)
  }

  def fields: List[String]
}

private[mist] case class JobDescription(jobDetails: JobDetails) extends Description {

  override def fields: List[String] = {
    List(jobDetails.jobId, new DateTime(jobDetails.startTime).toString, jobDetails.configuration.namespace, jobDetails.configuration.externalId.getOrElse(" " * 10), jobDetails.configuration.route.getOrElse(" " * 6))    
  }

}

private[mist] case class WorkerDescription(uid: String, namespace: String, address: String, blackSpot: Boolean) extends Description {

  override def fields: List[String] = {
    List(uid, namespace, address, blackSpot.toString)
  }

}
