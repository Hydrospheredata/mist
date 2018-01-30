package io.hydrosphere.mist.core.jvmjob

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.utils.{Err, TryLoad, Succ}
import mist.api.internal.BaseJobInstance

import scala.reflect.runtime.universe._

class JobsLoader(val classLoader: ClassLoader) {

  def loadJobInstance(className: String, action: Action): TryLoad[BaseJobInstance] = {
    loadClass(className).flatMap({
      case clz if mist.api.internal.JobInstance.isScalaInstance(clz) =>
        TryLoad(mist.api.internal.JobInstance.loadScala(clz))
      case clz if mist.api.internal.JobInstance.isJavaInstance(clz) =>
        TryLoad(mist.api.internal.JobInstance.loadJava(clz))
      case clz =>
        loadJobInstance(clz, action) match {
          case Some(i) => Succ(i)
          case None =>
            val e = new IllegalStateException(s"Can not instantiate job for action $action")
            Err(e)
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

  private def loadClass(name: String): TryLoad[Class[_]] =
    TryLoad(Class.forName(name, false, classLoader))
}

object JobsLoader {

  val Common = new JobsLoader(this.getClass.getClassLoader)

  def fromJar(file: File): JobsLoader = {
    val url = file.toURI.toURL
    val loader = new URLClassLoader(Array(url), getClass.getClassLoader)
    new JobsLoader(loader)
  }

}

