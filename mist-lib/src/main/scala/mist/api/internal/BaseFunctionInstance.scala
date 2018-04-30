package mist.api.internal

import mist.api._
import mist.api.data.{JsData, JsMap, JsNull}
import mist.api.jdsl.JMistFn

import scala.annotation.tailrec
import scala.util._

trait BaseFunctionInstance {

  def describe(): Seq[ArgInfo]

  def validateParams(params: JsMap): Extraction[Unit]

  def run(jobCtx: FullFnContext): Either[Throwable, JsData]
}

class ScalaFunctionInstance(instance: MistFn) extends BaseFunctionInstance {

  private val jobDef = instance.handle

  override def describe(): Seq[ArgInfo] = jobDef.describe()

  override def validateParams(params: JsMap): Extraction[Unit] = jobDef.validate(params)

  override def run(ctx: FullFnContext): Either[Throwable, JsData] = {
    try {
      instance.execute(ctx) match {
        case Success(data) => Right(data)
        case Failure(e) => Left(e)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }
}

class JavaFunctionInstance(instance: JMistFn) extends BaseFunctionInstance {

  private val jobDef = instance.handle.underlying

  override def describe(): Seq[ArgInfo] = jobDef.describe()

  override def validateParams(params: JsMap): Extraction[Unit] = jobDef.validate(params)

  override def run(ctx: FullFnContext): Either[Throwable, JsData] = {
    try {
      instance.execute(ctx) match {
        case Success(data) => Right(data)
        case Failure(e) => Left(e)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

}

object FunctionInstance {

  val NoOpInstance = new BaseFunctionInstance {

    override def run(jobCtx: FullFnContext): Either[Throwable, JsData] = Right(JsNull)

    override def validateParams(params: JsMap): Extraction[Unit] = Extracted(())

    override def describe(): Seq[ArgInfo] = Seq()
  }

  val ScalaJobClass = classOf[MistFn]
  val JavaJobClass = classOf[JMistFn]

  def isScalaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, ScalaJobClass)
  def isJavaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, JavaJobClass)

  def loadScala(clazz: Class[_]): ScalaFunctionInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistFn]
    new ScalaFunctionInstance(i)
  }

  def loadJava(clazz: Class[_]): JavaFunctionInstance = {
    val constr = clazz.getDeclaredConstructor()
    constr.setAccessible(true)
    val i = constr.newInstance()
    new JavaFunctionInstance(i.asInstanceOf[JMistFn])
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
