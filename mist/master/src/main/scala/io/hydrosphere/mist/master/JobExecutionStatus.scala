package io.hydrosphere.mist.master

case class JobExecutionStatus(
  id: String,
  namespace: String,
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized
)
