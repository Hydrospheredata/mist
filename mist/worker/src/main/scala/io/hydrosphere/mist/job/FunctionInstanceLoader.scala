package io.hydrosphere.mist.job

import java.io.File
import java.net.URLClassLoader

import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.job
import io.hydrosphere.mist.utils.{Err, TryLoad}

class FunctionInstanceLoader(val classLoader: ClassLoader) {

  def loadFnInstance(className: String, action: Action): TryLoad[JvmFunctionInstance] = {
    loadClass(className).flatMap({
      case clz if FunctionInstance.isInstance(clz) =>
        TryLoad(job.FunctionInstance.loadObject(clz))
          .orElse(TryLoad(job.FunctionInstance.loadClass(clz)))
      case _ =>
        val e = new IllegalStateException(s"Can not instantiate job class: $className for action $action")
        Err(e)
    })
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

