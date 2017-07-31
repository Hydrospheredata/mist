package io.hydrosphere.mist.jobs.jar

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.api.SetupConfiguration
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.utils.TypeAlias.JobResponse

import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

class JobsLoader(val classLoader: ClassLoader) {

  val v2Job = classOf[io.hydrosphere.mist.api.v2.MistJob]

  def loadJobClass(className: String): Try[JobClass] = {
    loadClass(className).map(clz => {
      if (isV2Job(clz)) {
        new JobClass(
          clazz = clz,
          execute = loadv2Job(className).toOption,
          train = None,
          serve = None
        )
      } else {
        val instance = new JobClass(
          clazz = clz,
          execute = loadJobInstance(clz, Action.Execute),
          train = loadJobInstance(clz, Action.Train),
          serve = loadJobInstance(clz, Action.Serve)
        )
        instance
      }
    })
  }

  def isV2Job(clz: Class[_]): Boolean = {
    clz.getInterfaces.contains(v2Job)
  }

  def loadv2Job(className: String): Try[JobInstance] = {
    loadClass(className).map(clz => {
      val inst = new JobInstance(clz, null) {
        override def run(conf: SetupConfiguration, params: Map[String, Any]): Either[Throwable, JobResponse] = {
          println("YOYOYO")
          val i = clz.getField("MODULE$").get(null).asInstanceOf[io.hydrosphere.mist.api.v2.MistJob]
          i.run.run(params, conf.context) match {
            case io.hydrosphere.mist.api.v2.JobSuccess(v) => Right(Map("result" -> v))
            case io.hydrosphere.mist.api.v2.JobFailure(e) => Left(e)
          }
        }

      }

      println("YOYOYO112")
      inst
    })
  }

  def loadJobInstance(className: String, action: Action): Try[JobInstance] = {
    loadClass(className).flatMap(clz => {
      if (isV2Job(clz)) {
        loadv2Job(className)
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
    case Action.Train => "train"
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
