package io.hydrosphere.mist.cli

import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.master.WorkerLink
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

private[mist] object JobDescription {
  val headers: List[String] = List("UID", "START TIME", "NAMESPACE", "EXT ID", "ROUTE", "SOURCE", "STATUS")
}

private[mist] case class JobDescription(jobDetails: JobDetails) extends Description {

  override def fields: List[String] = {
    List(jobDetails.jobId, jobDetails.startTime.map(new DateTime(_).toString).getOrElse(""), jobDetails.configuration.namespace, jobDetails.configuration.externalId.getOrElse(" " * 10), jobDetails.configuration.route.getOrElse(" " * 6), jobDetails.source.toString, jobDetails.status.toString)    
  }

}

private[mist] object WorkerDescription {
  val headers: List[String] = List("UID", "NAMESPACE", "ADDRESS", "BLACK SPOT")
}

private[mist] case class WorkerDescription(workerLink: WorkerLink) extends Description {

  override def fields: List[String] = {
    List(workerLink.uid, workerLink.name, workerLink.address, workerLink.blackSpot.toString)
  }

}

private[mist] object RouteDescription {
  val headers: List[String] = List("ROUTE", "NAMESPACE", "PATH", "CLASS NAME")
  
  
}

private[mist] case class RouteDescription(route: String, namespace: String, path: String, className: String) extends Description {
  
  override def fields: List[String] = {
    List(route, namespace, path, className)
  }
  
}
