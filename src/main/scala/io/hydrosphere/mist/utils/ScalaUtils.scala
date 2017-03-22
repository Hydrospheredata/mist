package io.hydrosphere.mist.utils

/**
  * Created by bulat on 22.03.17.
  */
object ScalaUtils {
  def companionOf(classz: Class[_]) : Any ={
    println(s"Class: $classz")
    val companionClassName = classz.getName + "$"
    val companionClass = Class.forName(companionClassName)
    println(s"Companion class: $companionClass")
    val moduleField = companionClass.getField("MODULE$")
    moduleField.get(null)
  }

}
