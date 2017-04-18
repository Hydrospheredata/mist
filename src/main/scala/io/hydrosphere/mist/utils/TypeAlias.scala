package io.hydrosphere.mist.utils

object TypeAlias {

  type JobParameters = Map[String, Any]
  val JobParameters = Map
  
  type JobResponse = Map[String, Any]
  type JobResponseOrError = Either[Map[String, Any], String]
  
}
