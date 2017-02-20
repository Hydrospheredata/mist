package io.hydrosphere.mist.utils

import java.io.File
import java.net.{URL, URLClassLoader}

import io.hydrosphere.mist.lib._

import scala.collection.immutable.Seq
import scala.reflect.runtime.universe._

case class ExternalMethodArgument(name: String, tpe: Type)

class ExternalMethod(methodName: String, private val cls: Class[_], private val objectRef: AnyRef) {

  // TODO: support classes 
  // Scala `object` reference of user job

  def run(parameters: Map[String, Any]): Any = {
    val args: Seq[Any] = arguments.map((param) => {
      if (param.tpe.erasure =:= typeOf[Option[Any]]) {
        parameters.get(param.name)
      } else {
        if (!parameters.contains(param.name)) {
          // TODO: special exception
          throw new Exception(s"${param.name} is required")
        }
        parameters(param.name.toString)
      }
    })
    val method = objectRef.getClass.getMethods.find(_.getName == methodName).get
    // TODO: more clear exceptions instead of InvocationTargetException 
    method.invoke(objectRef, args.asInstanceOf[Seq[AnyRef]]: _*).asInstanceOf[Map[String, Any]]
  }

  def arguments: List[ExternalMethodArgument] = {
    val params = symbol.paramss.head
    params.map((param) => ExternalMethodArgument(param.name.toString, param.typeSignature))
  }
  
  private def symbol: MethodSymbol = {
    val memberSymbol = runtimeMirror(cls.getClassLoader).classSymbol(cls).toType.member(newTermName(methodName))
    if (!memberSymbol.isMethod) {
      throw new Exception(s"MistJob subclass must implement $methodName method. See docs for details.")
    }
    memberSymbol.asMethod
  }
  
}

class ExternalInstance(val externalClass: ExternalClass, private val cls: Class[_]) {

  val objectRef: AnyRef = cls.getField("MODULE$").get(None)

  def getMethod(methodName: String): ExternalMethod = {
    new ExternalMethod(methodName, cls, objectRef)
  }

}

class ExternalClass(className: String, private val classLoader: ClassLoader) {
  
  private val cls: Class[_] = classLoader.loadClass(className)

  def getNewInstance: ExternalInstance = {
    new ExternalInstance(this, cls)
  }
  
  def isMistJob: Boolean = cls.getInterfaces.contains(classOf[MistJob])
  def isMLJob: Boolean = cls.getInterfaces.contains(classOf[MLMistJob])
  def isStreamingJob: Boolean = cls.getInterfaces.contains(classOf[StreamingSupport])
  def isSqlJob: Boolean = cls.getInterfaces.contains(classOf[SQLSupport])
  def isHiveJob: Boolean = cls.getInterfaces.contains(classOf[HiveSupport])
  
}

class ExternalJar(jarFile: File) {

  def getExternalClass(className: String): ExternalClass = {
    new ExternalClass(className, new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader))
  }

}

object ExternalJar {

  def apply(jarPath: String): ExternalJar = new ExternalJar(new File(jarPath))

  def apply(jarFile: File): ExternalJar = new ExternalJar(jarFile)
  
}
