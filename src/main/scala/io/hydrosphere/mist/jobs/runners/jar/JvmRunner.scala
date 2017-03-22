package io.hydrosphere.mist.jobs.runners.jar

import java.io.File
import java.lang.reflect.Method
import java.net.URLClassLoader

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.utils.TypeAlias.JobResponse

import cats.implicits._

import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * Job instance for Scala
  * Support only objects(singletons)
  *
  * @param clazz  - original class
  * @param method - target method for invocation (execute, train, serve)
  */
class JvmJobInstance(clazz: Class[_], method: Method) {

  def run(conf: SetupConfiguration, params: Map[String, Any]): Either[Throwable, JobResponse] = {
    for {
      instance <- Either.catchNonFatal(createInstance(conf))
      args     <- validateParams(params)
      response <- Either.catchNonFatal(invokeMethod(instance, args))
      _        <- Either.catchNonFatal(instance.stop())
    } yield response
  }

  private def createInstance(conf: SetupConfiguration): ContextSupport = {
    val i = clazz.getField("MODULE$").get(null).asInstanceOf[ContextSupport]
    i.setup(conf)
    i
  }

  private def invokeMethod(inst: ContextSupport, args: Seq[AnyRef]): JobResponse = {
    println(inst)
    println(args)
    method.invoke(inst, args: _*).asInstanceOf[JobResponse]
  }

  def validateParams(params: Map[String, Any]): Either[Throwable, Seq[AnyRef]] = {
    val validated: Seq[Either[Throwable, Any]] = arguments.toSeq.map({ case (name, tpe) =>
      val param: Option[Any] = params.get(name)
      param match {
        // ignore optional arguments if they not presented
        case x if tpe.erasure =:= typeOf[Option[Any]] => Right(param)
        case Some(x) => Right(x)
        case None =>
          val msg = s"Missing argument name: $name, type: $tpe"
          Left(new IllegalArgumentException(msg))
      }
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

  def arguments: Map[String, Type] = {
    val termName = newTermName(method.getName)
    val symbol = runtimeMirror(clazz.getClassLoader).classSymbol(clazz).toType.member(termName)
    symbol.asMethod.paramss.head.map(s => s.name.toString -> s.typeSignature).toMap
  }
}

object JvmJobLoader {

  def load(name: String, action: Action, loader: ClassLoader): Try[JvmJobInstance] = {
    val targetName = methodNameByAction(action)
    Try {
      val clazz = Class.forName(name, true, loader)

      val interfaces = clazz.getInterfaces
      val targetClass = classForAction(action)

      if (!interfaces.contains(targetClass)) {
        val msg = s"Class: $name should implement ${targetClass.getName}"
        throw new IllegalArgumentException(msg)
      }

      val method = clazz.getMethods.find(_.getName == targetName)

      method match {
        case Some(m) => new JvmJobInstance(clazz, m)
        case None =>
          val msg = s"Class: $name should have method $targetName"
          throw new RuntimeException(msg)
      }
    }
  }

  def loadFromJar(name: String, action: Action, file: File): Try[JvmJobInstance] = {
    val url = file.toURI.toURL
    val loader = new URLClassLoader(Array(url), getClass.getClassLoader)
    load(name, action, loader)
  }

  def methodNameByAction(action: Action): String = action match {
    case Action.Execute => "execute"
    case Action.Train => "train"
    case Action.Serve => "serve"
  }

  def classForAction(action: Action): Class[_] = action match {
    case Action.Execute => classOf[MistJob]
    case Action.Train | Action.Serve => classOf[MLMistJob]
  }

}

