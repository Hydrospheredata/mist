package io.hydrosphere.mist.core.jvmjob

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.core.jvmjob.JobClass._
import mist.api.internal.BaseJobInstance
//TODO: move logic to JobInstance. and remove the file
case class JobClass(
  clazz: Class[_],
  execute: Option[BaseJobInstance],
  serve: Option[BaseJobInstance]
) {

  def isValid: Boolean = execute.nonEmpty || serve.nonEmpty

  //TODO: works only old
  def supportedClasses(): Seq[Class[_]] = {
    val interfaces = clazz.getInterfaces
    MistClasses.filter(interfaces.contains)
  }
}

object JobClass {

  val MistClasses = Seq(
    classOf[MistJob],
    classOf[MLMistJob],
    classOf[StreamingSupport],
    classOf[SQLSupport],
    classOf[HiveSupport]
  )
}
