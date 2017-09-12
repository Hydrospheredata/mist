package io.hydrosphere.mist.master

import io.hydrosphere.mist.Messages.WorkerMessages.WorkerInitInfo
import io.hydrosphere.mist.jobs.JobDetails

case class WorkerLink(
  name: String,
  address: String,
  sparkUi: Option[String]
)

case class WorkerFullInfo(
  name: String,
  jobs: Seq[JobDetails],
  initInfo: Option[WorkerInitInfo]
)
