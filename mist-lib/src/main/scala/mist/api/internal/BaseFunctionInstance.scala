package mist.api.internal

import mist.api._
import mist.api.data.{JsData, JsMap, JsNull}

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

//class JavaFunctionInstance(instance: JMistFn) extends BaseFunctionInstance {
//
//  private val jobDef = instance.handle.underlying
//
//  override def describe(): Seq[ArgInfo] = jobDef.describe()
//
//  override def validateParams(params: JsMap): Extraction[Unit] = jobDef.validate(params)
//
//  override def run(ctx: FullFnContext): Either[Throwable, JsData] = {
//    try {
//      instance.execute(ctx) match {
//        case Success(data) => Right(data)
//        case Failure(e) => Left(e)
//      }
//    } catch {
//      case e: Throwable => Left(e)
//    }
//  }
//
//}

class PythonFunctionInstance(args: Seq[ArgInfo]) extends BaseFunctionInstance {

  override def run(jobCtx: FullFnContext): Either[Throwable, JsData] =
    Left(new RuntimeException("could not execute from here"))

  override def validateParams(params: JsMap): Extraction[Unit] = {
    val errors = args
      .collect { case x: UserInputArgument => x }
      .map(arg => validate(arg, params))

    if (errors.exists(_.isFailed)) {
      Failed.ComplexFailure(errors.collect({case f: Failed => f}))
    } else {
      Extracted.unit
    }
  }

  private def isOptionalArg(argType: ArgType): Boolean = argType match {
    case MOption(_) => true
    case _ => false
  }

  private def validate(arg: UserInputArgument, params: JsMap): Extraction[Unit] = {
    params.fieldValue(arg.name) match {
      case JsNull if isOptionalArg(arg.t) => Extracted.unit
      case JsNull => Failed.InvalidField(arg.name, Failed.InvalidType(arg.t.toString, "null"))
      case _ => Extracted.unit
    }
  }

  override def describe(): Seq[ArgInfo] = args
}

object FunctionInstance {

  val NoOpInstance = new BaseFunctionInstance {

    override def run(jobCtx: FullFnContext): Either[Throwable, JsData] = Right(JsNull)

    override def validateParams(params: JsMap): Extraction[Unit] = Extracted(())

    override def describe(): Seq[ArgInfo] = Seq()
  }

  val ScalaJobClass = classOf[MistFn]
//  val JavaJobClass = classOf[JMistFn]

  def isInstance(clazz: Class[_]): Boolean = implementsClass(clazz, ScalaJobClass)
//  def isJavaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, JavaJobClass)

  def loadObject(clazz: Class[_]): ScalaFunctionInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistFn]
    new ScalaFunctionInstance(i)
  }

  def loadClass(clazz: Class[_]): ScalaFunctionInstance = {
    val constr = clazz.getDeclaredConstructor()
    constr.setAccessible(true)
    val i = constr.newInstance()
    new ScalaFunctionInstance(i.asInstanceOf[MistFn])
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
