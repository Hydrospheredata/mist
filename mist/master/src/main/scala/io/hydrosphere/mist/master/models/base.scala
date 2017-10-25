package io.hydrosphere.mist.master.models

import scala.concurrent.duration.Duration

trait NamedConfig {
  val name: String
}

case class ContextConfig(
  name: String,
  sparkConf: Map[String, String],
  downtime: Duration,
  maxJobs: Int,
  precreated: Boolean,
  runOptions: String,
  workerMode: String,
  streamingDuration: Duration
) extends NamedConfig

case class EndpointConfig(
  name: String,
  path: String,
  className: String,
  defaultContext: String
) extends NamedConfig
