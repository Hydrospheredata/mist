package io.hydrosphere.mist.master.models

import io.hydrosphere.mist.jobs.JobInfo

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
  streamingDuration: Duration
) extends NamedConfig

case class EndpointConfig(
  name: String,
  path: String,
  className: String,
  defaultContext: String
) extends NamedConfig


case class FullEndpointInfo(config: EndpointConfig, info: JobInfo)
