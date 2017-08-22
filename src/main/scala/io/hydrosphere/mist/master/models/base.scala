package io.hydrosphere.mist.master.models

import io.hydrosphere.mist.jobs.jar.JobClass
import io.hydrosphere.mist.jobs.{Action, JobInfo, JvmJobInfo, PyJobInfo}
import cats.implicits._
import io.hydrosphere.mist.worker.{Shared, WorkerMode}

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


case class FullEndpointInfo(config: EndpointConfig, info: JobInfo)
