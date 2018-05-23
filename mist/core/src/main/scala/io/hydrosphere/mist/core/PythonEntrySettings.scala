package io.hydrosphere.mist.core

case class PythonEntrySettings(
  driver: String,
  global: String
)

object PythonEntrySettings {
  val PythonDriverKey = "spark.pyspark.driver.python"
  val PythonGlobalKey = "spark.pyspark.python"
  val PythonDefault = "python"

  def fromConf(conf: Map[String, String]): PythonEntrySettings = {
    val driver = conf.get(PythonDriverKey).orElse(conf.get(PythonGlobalKey)).getOrElse(PythonDefault)
    val global = conf.getOrElse(PythonGlobalKey, PythonDefault)
    PythonEntrySettings(driver, global)
  }
}

