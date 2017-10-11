package io.hydrosphere.mist.core.jvmjob

import mist.api.data.MData
import mist.api.{ArgInfo, JobContext, UserInputArgument, data => mdata}
import mist.api.internal.BaseJobInstance

import scala.util._

class OldInstanceWrapper(oldInstance: JobInstance) extends BaseJobInstance {

  override def run(jobCtx: JobContext): Either[Throwable, MData] = {
    oldInstance.run(jobCtx.setupConfiguration, jobCtx.params) match {
      case l: Left[_, _] => l.asInstanceOf[Either[Throwable, MData]]
      case Right(anyMap) => Try(MData.fromAny(anyMap)) match {
        case Success(data) => Right(data)
        case Failure(e) => Left(e)
      }
    }
  }

  override def describe(): Seq[ArgInfo] = {
    oldInstance.argumentsTypes.map({case (name, t) => UserInputArgument(name, t)}).toSeq
  }

  override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    oldInstance.validateParams(params) match {
      case Right(_) => Right(params)
      case Left(e) => Left(e)
    }
  }

}
