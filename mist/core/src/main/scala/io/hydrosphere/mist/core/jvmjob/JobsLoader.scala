package io.hydrosphere.mist.core.jvmjob

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import mist.api.internal.BaseJobInstance

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

class JobsLoader(val classLoader: ClassLoader) {

  //TODO: remove unused method
  def loadJobClass(className: String): Try[JobClass] = {
    loadClass(className).map({
      case clz if mist.api.internal.JobInstance.isScalaInstance(clz) =>
        JobClass(clz, execute = Option(mist.api.internal.JobInstance.loadScala(clz)), serve = None)
      case clz if mist.api.internal.JobInstance.isJavaInstance(clz) =>
        JobClass(clz, execute = Option(mist.api.internal.JobInstance.loadJava(clz)), serve = None)
      case clz =>
        new JobClass(
          clazz = clz,
          execute = loadJobInstance(clz, Action.Execute),
          serve = loadJobInstance(clz, Action.Serve)
        )
    })
  }

  def loadJobInstance(className: String, action: Action): Try[BaseJobInstance] = {
    loadClass(className).flatMap({
      case clz if mist.api.internal.JobInstance.isScalaInstance(clz) =>
        Try(mist.api.internal.JobInstance.loadScala(clz))
      case clz if mist.api.internal.JobInstance.isJavaInstance(clz) =>
        Try(mist.api.internal.JobInstance.loadJava(clz))
      case clz =>
        loadJobInstance(clz, action) match {
          case Some(i) => Success(i)
          case None =>
            val e = new IllegalStateException(s"Can not instantiate job for action $action")
            Failure(e)
        }
    })
  }

  private def loadJobInstance(clazz: Class[_], action: Action): Option[OldInstanceWrapper] = {
    val methodName = methodNameByAction(action)
    val term = newTermName(methodName)
    val symbol = runtimeMirror(clazz.getClassLoader).classSymbol(clazz).toType.member(term)
    if (!symbol.isMethod) {
      None
    } else {
      val instance = new JobInstance(clazz, symbol.asMethod)
      Some(new OldInstanceWrapper(instance))
    }
  }

  private def methodNameByAction(action: Action): String = action match {
    case Action.Execute => "execute"
    case Action.Serve => "serve"
  }

  private def loadClass(name: String): Try[Class[_]] = {
    try {
      val clazz = Class.forName(name, false, classLoader)
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

