package mist.api.internal

import mist.api._
import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.jdsl.JMistJob

import scala.annotation.tailrec

trait BaseJobInfo {

  def describe(): Seq[ArgInfo]

  def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]]
}

trait BaseJobInstance extends BaseJobInfo {

  def run(jobCtx: JobContext): Either[Throwable, JsLikeData]

}

class ScalaJobInstance(instance: MistJob[_]) extends BaseJobInstance {

  override def describe(): Seq[ArgInfo] = Seq.empty

  override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    Right(params)
  }

  override def run(ctx: JobContext): Either[Throwable, JsLikeData] = {
    try {
      instance.execute(ctx) match {
        case f: JobFailure[_] => Left(f.e)
        case JobSuccess(data) => Right(data)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

}

class JavaJobInstance(instance: JMistJob[_]) extends BaseJobInstance {

  override def describe(): Seq[ArgInfo] = Seq.empty

  override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
    Right(params)
  }

  override def run(ctx: JobContext): Either[Throwable, JsLikeData] = {
    try {
      instance.execute(ctx) match {
        case f: JobFailure[_] => Left(f.e)
        case JobSuccess(data) => Right(data)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

}

object JobInstance {
  val NoOpInstance = new BaseJobInstance {
    override def run(jobCtx: JobContext): Either[Throwable, JsLikeData] = Right(JsLikeNull)

    override def validateParams(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = Right(Map.empty)

    override def describe(): Seq[ArgInfo] = Seq()
  }

  val ScalaJobClass = classOf[MistJob[_]]
  val JavaJobClass = classOf[JMistJob[_]]

  def isScalaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, ScalaJobClass)
  def isJavaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, JavaJobClass)

  def loadScala(clazz: Class[_]): ScalaJobInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistJob[_]]
    new ScalaJobInstance(i)
  }

  def loadJava(clazz: Class[_]): JavaJobInstance = {
    val constr = clazz.getDeclaredConstructor()
    constr.setAccessible(true)
    val i = constr.newInstance()
    new JavaJobInstance(i.asInstanceOf[JMistJob[_]])
  }

  @tailrec
  def implementsClass(a: Class[_], parent: Class[_]): Boolean = {
    val aParent = a.getSuperclass
    if (aParent != parent) {
      if (aParent != classOf[java.lang.Object])
        implementsClass(aParent, parent)
      else
        false
    } else {
      true
    }
  }
}
