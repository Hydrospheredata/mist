package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.models.JobDetailsLink

case class WorkerLink(
  name: String,
  address: String,
  sparkUi: Option[String],
  initInfo: WorkerInitInfo
)

case class WorkerFullInfo(
  name: String,
  address: String,
  sparkUi: Option[String],
  jobs: Seq[JobDetailsLink],
  initInfo: WorkerInitInfo
)
