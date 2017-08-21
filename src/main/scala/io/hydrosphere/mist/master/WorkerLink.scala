package io.hydrosphere.mist.master

case class WorkerLink(
  name: String,
  address: String,
  sparkUi: Option[String]
)
