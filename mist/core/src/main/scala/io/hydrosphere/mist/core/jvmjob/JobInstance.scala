package io.hydrosphere.mist.core.jvmjob

import java.lang.reflect.InvocationTargetException

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.utils.EitherOps
import EitherOps._
import mist.api.args.ArgType

import scala.reflect.runtime.universe._

/**
  * Job instance for Scala
  * Support only objects(singletons)
  *
  * @param clazz  - original class
  * @param method - target method for invocation (execute, train, serve)
  */
class JobInstance(clazz: Class[_], method: MethodSymbol) {

  def run(conf: SetupConfiguration, params: Map[String, Any]): Either[Throwable, Map[String, Any]] =
    for {
      args     <- validateParams(params)
      instance <- Either.catchNonFatal(createInstance(conf))
      response <- Either.catchAll(invokeMethod(instance, args))
      _        <- Either.catchNonFatal(instance.stop())
    } yield response

  private def createInstance(conf: SetupConfiguration): ContextSupport = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[ContextSupport]
    i.setup(conf)
    i
  }

  private def invokeMethod(inst: ContextSupport, args: Seq[AnyRef]): Map[String, Any] = {
    val name = method.fullName.split('.').last
    val target = clazz.getMethods.find(_.getName == name)
    target match {
      case Some(m) =>
        try {
          m.invoke(inst, args: _*).asInstanceOf[Map[String, Any]]
        } catch {
          case e: InvocationTargetException => throw e.getTargetException
          case e: Throwable => throw e
        }
      case None =>
        throw new IllegalStateException(s"Class $clazz does not have method $name")
    }
  }

  def validateParams(params: Map[String, Any]): Either[Throwable, Seq[AnyRef]] = {
    val validated: Seq[Either[Throwable, Any]] = arguments.map({case (name, tpe) =>
      val param = params.get(name)
      validateParam(tpe, name, param)
    })

    if (validated.exists(_.isLeft)) {
      val errors = validated.collect({ case Left(e) => e.getMessage }).mkString("(", ",", ")")
      val msg = s"Param validation errors: $errors"
      Left(new IllegalArgumentException(msg))
    } else {
      val p = validated.collect({case Right(x) => x.asInstanceOf[AnyRef]})
      Right(p)
    }
  }

  private def validateParam(tpe: Type, name: String, value: Option[Any]): Either[Throwable, Any] = {
    value match {
      // ignore optional arguments if they not presented
      case x if tpe.erasure =:= typeOf[Option[Any]] => Right(value)
      case Some(x) => Right(x)
      case None =>
        val msg = s"Missing argument name: $name, type: $tpe"
        Left(new IllegalArgumentException(msg))
    }
  }

  private def arguments: Seq[(String, Type)] =
    method.paramss.head.map(s => s.name.toString -> s.typeSignature)

  def argumentsTypes: Map[String, ArgType] =
    arguments.toMap.mapValues(ArgType.fromType)
}
