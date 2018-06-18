package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo

case class WorkerLink(
  name: String,
  address: String,
  sparkUi: Option[String],
  initInfo: WorkerInitInfo
)

