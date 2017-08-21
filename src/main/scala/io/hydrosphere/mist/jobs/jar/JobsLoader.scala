package io.hydrosphere.mist.jobs.jar

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.jobs.Action

import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

class JobsLoader(val classLoader: ClassLoader) {

  def loadJobClass(className: String): Try[JobClass] = {
    loadClass(className).map(clz => {
      val instance = new JobClass(
        clazz = clz,
        execute = loadJobInstance(clz, Action.Execute),
        serve = loadJobInstance(clz, Action.Serve)
      )
      instance
    })
  }

  def loadJobInstance(className: String, action: Action): Try[JobInstance] = {
    loadClass(className).flatMap(clz => {
      loadJobInstance(clz, action) match {
        case Some(i) => Success(i)
        case None =>
          val e = new IllegalStateException(s"Can not instantiate job for action $action")
          Failure(e)
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
