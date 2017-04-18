package io.hydrosphere.mist.lib.spark2.ml

/**
  * Created by bulat on 22.03.17.
  */
object ScalaUtils {
  def companionOf(classz: Class[_]) : Any ={
    val companionClassName = classz.getName + "$"
    val companionClass = Class.forName(companionClassName)
    val moduleField = companionClass.getField("MODULE$")
    moduleField.get(null)
  }
}
