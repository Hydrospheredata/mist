package io.hydrosphere.mist.master.models

import io.hydrosphere.mist.jobs.jar.JobClass
import io.hydrosphere.mist.jobs.{JvmJobInfo, PyJobInfo, Action, JobInfo}
import cats.implicits._

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


case class FullEndpointInfo(config: EndpointConfig, info: JobInfo) { self =>

  def validateAction(params: Map[String, Any], action: Action): Either[Throwable, FullEndpointInfo] = {

    def checkJvm(jvmJobInfo: JvmJobInfo): Either[Throwable, FullEndpointInfo] = {
      val jobClass = jvmJobInfo.jobClass
      val inst = action match {
        case Action.Execute => jobClass.execute
        case Action.Train => jobClass.train
        case Action.Serve => jobClass.serve
      }
      inst match {
        case None => Left(new IllegalStateException(s"Job without $action job instance"))
        case Some(exec) => exec.validateParams(params).map(_ => self)
      }
    }

    info match {
      case PyJobInfo => Right(self)
      case jvm: JvmJobInfo => checkJvm(jvm)
    }
  }
}
