package mist.api.internal

import mist.api._
import mist.api.args.{ArgInfo, ArgType, MOption, UserInputArgument}
import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.jdsl.JMistFn

import scala.annotation.tailrec

trait BaseFunctionInstance {

  def describe(): Seq[ArgInfo]

  def validateParams(params: Map[String, Any]): Either[Throwable, Any]

  def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData]
}

class ScalaFunctionInstance(instance: MistFn[_]) extends BaseFunctionInstance {

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

class JavaFunctionInstance(instance: JMistFn[_]) extends BaseFunctionInstance {

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

class PythonFunctionInstance(args: Seq[ArgInfo]) extends BaseFunctionInstance {

  override def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData] =
    Left(new RuntimeException("could not execute from here"))

  override def validateParams(params: Map[String, Any]): Either[Throwable, Any] = {
    val errors = args
      .collect { case x: UserInputArgument => x }
      .map(arg => validate(arg, params))

    if (errors.exists(_.isLeft)) {
      val errorMsg = errors.collect({ case Left(e) => e}).mkString("(", ",", ")")
      val msg = s"Param validation errors: $errorMsg"
      Left(new IllegalArgumentException(msg))
    } else {
      Right(())
    }
  }

  private def isOptionalArg(argType: ArgType): Boolean = argType match {
    case MOption(_) => true
    case _ => false
  }

  private def validate(arg: UserInputArgument, params: Map[String, Any]): Either[String, Unit] = {
    params.get(arg.name) match {
      case Some(_) if isOptionalArg(arg.t) => Right(())
      case Some(_) =>
        Right(())
      case None =>
        Left(s"Missing parameter by name: ${arg.name}")
    }
  }

  override def describe(): Seq[ArgInfo] = args
}

object FunctionInstance {

  val NoOpInstance = new BaseFunctionInstance {

    override def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData] = Right(JsLikeNull)

    override def validateParams(params: Map[String, Any]): Either[Throwable, Any] = Right(Map.empty)

    override def describe(): Seq[ArgInfo] = Seq()
  }

  val ScalaJobClass = classOf[MistFn[_]]
  val JavaJobClass = classOf[JMistFn[_]]

  def isScalaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, ScalaJobClass)
  def isJavaInstance(clazz: Class[_]): Boolean = implementsClass(clazz, JavaJobClass)

  def loadScala(clazz: Class[_]): ScalaFunctionInstance = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[MistFn[_]]
    new ScalaFunctionInstance(i)
  }

  def loadJava(clazz: Class[_]): JavaFunctionInstance = {
    val constr = clazz.getDeclaredConstructor()
    constr.setAccessible(true)
    val i = constr.newInstance()
    new JavaFunctionInstance(i.asInstanceOf[JMistFn[_]])
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
