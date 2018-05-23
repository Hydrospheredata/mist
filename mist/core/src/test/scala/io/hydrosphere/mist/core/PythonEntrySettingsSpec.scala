package io.hydrosphere.mist.core

import org.scalatest.{FunSpec, Matchers}

class PythonEntrySettingsSpec extends FunSpec with Matchers{

  it("should correctly extract python settings") {
    import PythonEntrySettings._
    fromConf(Map.empty) shouldBe PythonEntrySettings("python", "python")
    fromConf(Map("spark.pyspark.python" -> "python3")) shouldBe PythonEntrySettings("python3", "python3")
    fromConf(Map("spark.pyspark.driver.python" -> "python3")) shouldBe PythonEntrySettings("python3", "python")
  }
}
