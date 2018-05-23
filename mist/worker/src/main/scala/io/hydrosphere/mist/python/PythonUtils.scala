package io.hydrosphere.mist.python

import mist.api._


object PythonUtils {
  import scala.collection.JavaConverters._


  def toSeq[A](list: java.util.List[A]): Seq[A] = {
    list.asScala
  }

  def toSeq[A](coll: java.util.Collection[A]): Seq[A] = {
    coll.asScala.toSeq
  }

  val doubleType: ArgType = MDouble
  val anyType: ArgType = MAny

  val strType: ArgType = MString
  val intType: ArgType = MInt
  val boolType: ArgType = MBoolean

  def listType(underlying: ArgType): ArgType = MList(underlying)
  def optType(underlying: ArgType): ArgType = MOption(underlying)

  def userArg(name: String, argType: ArgType): ArgInfo = UserInputArgument(name, argType)

  def systemArg(tags: java.util.HashSet[String]): ArgInfo = InternalArgument(tags.asScala.toSeq)

}
