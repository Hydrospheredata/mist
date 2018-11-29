package io.hydrosphere.mist.master.models

import io.hydrosphere.mist.common.CommonData.WorkerInitInfo

case class WorkerLink(
  name: String,
  address: String,
  sparkUi: Option[String],
  initInfo: WorkerInitInfo
)

case class ClusterLink(
  name: String,
  info: String,
  workers: Seq[WorkerLink]
)