package io.hydrosphere.mist.core.jvmjob

import mist.api.data.JsLikeData
import mist.api.internal.BaseFunctionInstance
import mist.api.{FullFnContext, data => mdata}
import mist.api.args.ArgInfo
import scala.util._

class OldInstanceWrapper(oldInstance: FunctionInstance) extends BaseFunctionInstance {

  override def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData] = {
    oldInstance.run(jobCtx.setupConf, jobCtx.params) match {
      case l: Left[_, _] => l.asInstanceOf[Either[Throwable, JsLikeData]]
      case Right(anyMap) => Try(JsLikeData.fromScala(anyMap)) match {
        case Success(data) => Right(data)
        case Failure(e) => Left(e)
      }
    }
  }

  override def describe(): Seq[ArgInfo] = oldInstance.arguments

  override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    oldInstance.validateParams(params) match {
      case Right(_) => Right(params)
      case Left(e) => Left(e)
    }
  }

}
