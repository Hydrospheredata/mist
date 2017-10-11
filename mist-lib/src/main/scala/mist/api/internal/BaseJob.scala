package mist.api.internal

import mist.api._
import mist.api.data.MData

trait BaseJobInfo {

  def describe(): Seq[ArgInfo]

  def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]]
}

trait BaseJobInstance extends BaseJobInfo {

  def run(jobCtx: JobContext): Either[Throwable, MData]

}

class V2JobInstance(instance: MistJob[_]) extends BaseJobInstance {

  override def describe(): Seq[ArgInfo] = instance.defineJob.describe()

  override def run(ctx: JobContext): Either[Throwable, MData] = {
    try {
      instance.execute(ctx) match {
        case f: JobFailure[_] => Left(f.e)
        case JobSuccess(data) => Right(data)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

  override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    instance.defineJob.validate(params)
  }
}

object V2JobInstance {

  def isScalaInstance(clazz: Class[_]): Boolean = clazz.getSuperclass == classOf[mist.api.MistJob[_]]

  def load(clazz: Class[_]): V2JobInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistJob[_]]
    new V2JobInstance(i)
  }
}
