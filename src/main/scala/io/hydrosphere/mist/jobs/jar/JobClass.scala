package io.hydrosphere.mist.jobs.jar

import io.hydrosphere.mist.api._

import JobClass._

case class JobClass(
  clazz: Class[_],
  execute: Option[JobInstance],
  serve: Option[JobInstance]
) {

  def isValid: Boolean = execute.nonEmpty || serve.nonEmpty

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
