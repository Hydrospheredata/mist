package io.hydrosphere.mist.core.jvmjob

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.utils.{Err, TryLoad, Succ}
import mist.api.internal.BaseFunctionInstance

import scala.reflect.runtime.universe._

class FunctionInstanceLoader(val classLoader: ClassLoader) {

  def loadFnInstance(className: String, action: Action): TryLoad[BaseFunctionInstance] = {
    loadClass(className).flatMap({
      case clz if mist.api.internal.FunctionInstance.isScalaInstance(clz) =>
        TryLoad(mist.api.internal.FunctionInstance.loadScala(clz))
      case clz if mist.api.internal.FunctionInstance.isJavaInstance(clz) =>
        TryLoad(mist.api.internal.FunctionInstance.loadJava(clz))
      case clz =>
        loadFnInstance(clz, action) match {
          case Some(i) => Succ(i)
          case None =>
            val e = new IllegalStateException(s"Can not instantiate job for action $action")
            Err(e)
        }
    })
  }

  private def loadFnInstance(clazz: Class[_], action: Action): Option[OldInstanceWrapper] = {
    val methodName = methodNameByAction(action)
    val term = newTermName(methodName)
    val symbol = runtimeMirror(clazz.getClassLoader).classSymbol(clazz).toType.member(term)
    if (!symbol.isMethod) {
      None
    } else {
      val instance = new FunctionInstance(clazz, symbol.asMethod)
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

object FunctionInstanceLoader {

  val Common = new FunctionInstanceLoader(this.getClass.getClassLoader)

  def fromJar(file: File): FunctionInstanceLoader = {
    val url = file.toURI.toURL
    val loader = new URLClassLoader(Array(url), getClass.getClassLoader)
    new FunctionInstanceLoader(loader)
  }

}

