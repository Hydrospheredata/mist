package io.hydrosphere.mist.core.jvmjob

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.api.SetupConfiguration
import io.hydrosphere.mist.core.CommonData.Action
import mist.api.jdsl.JMistJob
import mist.api.{JobContext, MistJob}

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

class JobsLoader(val classLoader: ClassLoader) {

  val v2Job = classOf[MistJob[_]]

  def loadJobClass(className: String): Try[JobClass] = {
    loadClass(className).map(clazz => {
      clazz match {
        case clz if isV2Job(clz) =>
          new JobClass(
            clazz = clz,
            execute = loadv2Job(className).toOption,
            serve = None
          )
        case clz if isV2JavaJob(clz) =>
          new JobClass(
            clazz = clz,
            execute = loadV2JavaJob(className).toOption,
            serve = None
          )
        case clz =>
          new JobClass(
            clazz = clz,
            execute = loadJobInstance(clz, Action.Execute),
            serve = loadJobInstance(clz, Action.Serve)
          )
      }
    })
  }

  def isV2Job(clz: Class[_]): Boolean = {
    clz.getInterfaces.contains(v2Job)
  }

  def isV2JavaJob(clz: Class[_]): Boolean = {
    clz.getSuperclass == classOf[JMistJob[_]]
  }

  def loadv2Job(className: String): Try[JobInstance] = {
    loadClass(className).map(clz => {
      new JobInstance(clz, null) {
        override def run(conf: SetupConfiguration, params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
          val i = clz.getField("MODULE$").get(null).asInstanceOf[MistJob[_]]
          val ctx = new JobContext(conf, params)
          i.defineJob.invoke(ctx) match {
            case mist.api.JobSuccess(v) => Right(Map("result" -> v))
            case mist.api.JobFailure(e) => Left(e)
          }
        }

        override def argumentsTypes: Map[String, JobArgType] = Map.empty

        override def validateParams(params: Map[String, Any]): Either[Throwable, Seq[AnyRef]] = {
          Right(Seq.empty)
        }
      }
    })
  }

  def loadV2JavaJob(className: String): Try[JobInstance] = {
    loadClass(className).map(clz => {
      new JobInstance(clz, null) {
        override def run(conf: SetupConfiguration, params: Map[String, Any]): Either[Throwable, Map[String, Any]] = {
          val constr = clz.getDeclaredConstructor();
          constr.setAccessible(true)
          val i = constr.newInstance().asInstanceOf[MistJob[_]]
          val ctx = new JobContext(conf, params)
          i.defineJob.invoke(ctx) match {
            case mist.api.JobSuccess(v) => Right(Map("result" -> v))
            case mist.api.JobFailure(e) => Left(e)
          }
        }

        override def argumentsTypes: Map[String, JobArgType] = Map.empty

        override def validateParams(params: Map[String, Any]): Either[Throwable, Seq[AnyRef]] = {
          Right(Seq.empty)
        }
      }
    })
  }

  def loadJobInstance(className: String, action: Action): Try[JobInstance] = {
    loadClass(className).flatMap(clz => {
      if (isV2Job(clz)) {
        loadv2Job(className)
      } else if (isV2JavaJob(clz)) {
        loadV2JavaJob(className)
      } else {
        loadJobInstance(clz, action) match {
          case Some(i) => Success(i)
          case None =>
            val e = new IllegalStateException(s"Can not instantiate job for action $action")
            Failure(e)
        }
      }
    })
  }

  private def loadJobInstance(clazz: Class[_], action: Action): Option[JobInstance] = {
    val methodName = methodNameByAction(action)
    val term = newTermName(methodName)
    val symbol = runtimeMirror(clazz.getClassLoader).classSymbol(clazz).toType.member(term)
    if (!symbol.isMethod) {
      None
    } else {
      val instance = new JobInstance(clazz, symbol.asMethod)
      Some(instance)
    }
  }

  private def methodNameByAction(action: Action): String = action match {
    case Action.Execute => "execute"
    case Action.Serve => "serve"
  }

  private def loadClass(name: String): Try[Class[_]] = {
    try {
      val clazz = Class.forName(name, true, classLoader)
      Success(clazz)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

}

object JobsLoader {

  val Common = new JobsLoader(this.getClass.getClassLoader)

  def fromJar(file: File): JobsLoader = {
    val url = file.toURI.toURL
    val loader = new URLClassLoader(Array(url), getClass.getClassLoader)
    new JobsLoader(loader)
  }

}
