package mist.api.internal

import mist.api._
import mist.api.args.ArgInfo
import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.jdsl.JMistFn

import scala.annotation.tailrec

trait BaseJobInstance {

  def describe(): Seq[ArgInfo]

  def validateParams(params: Map[String, Any]): Either[Throwable, Any]

  def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData]
}

class ScalaJobInstance(instance: MistFn[_]) extends BaseJobInstance {

  private val jobDef = instance.handle

  override def describe(): Seq[ArgInfo] = jobDef.describe()

  override def validateParams(params: Map[String, Any]): Either[Throwable, Any] =
    jobDef.validate(params)


  override def run(ctx: FullFnContext): Either[Throwable, JsLikeData] = {
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

class JavaJobInstance(instance: JMistFn[_]) extends BaseJobInstance {

  private val jobDef = instance.handle.underlying

  override def describe(): Seq[ArgInfo] = jobDef.describe()

  override def validateParams(params: Map[String, Any]): Either[Throwable, Any] =
    jobDef.validate(params)

  override def run(ctx: FullFnContext): Either[Throwable, JsLikeData] = {
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

    override def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData] = Right(JsLikeNull)

    override def validateParams(params: Map[String, Any]): Either[Throwable, Any] = Right(Map.empty)

    override def describe(): Seq[ArgInfo] = Seq()
  }

  val ScalaJobClass = classOf[MistFn[_]]
  val JavaJobClass = classOf[JMistFn[_]]

  def isScalaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, ScalaJobClass)
  def isJavaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, JavaJobClass)

  def loadScala(clazz: Class[_]): ScalaJobInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistFn[_]]
    new ScalaJobInstance(i)
  }

  def loadJava(clazz: Class[_]): JavaJobInstance = {
    val constr = clazz.getDeclaredConstructor()
    constr.setAccessible(true)
    val i = constr.newInstance()
    new JavaJobInstance(i.asInstanceOf[JMistFn[_]])
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
