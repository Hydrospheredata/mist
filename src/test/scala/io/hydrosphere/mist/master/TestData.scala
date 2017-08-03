package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory

object TestData {


  val contextSettings = {
    val cfg = ConfigFactory.parseString(
      """
        |context-defaults {
        | downtime = Inf
        | streaming-duration = 1 seconds
        | max-parallel-jobs = 20
        | precreated = false
        | spark-conf = { }
        | run-options = "--opt"
        |}
        |
        |context {
        |
        |  foo {
        |    spark-conf {
        |       spark.master = "local[2]"
        |    }
        |  }
        |}
      """.stripMargin)
    
    ContextsSettings(cfg)
  }
}
