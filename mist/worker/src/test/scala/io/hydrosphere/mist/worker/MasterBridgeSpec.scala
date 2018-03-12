package io.hydrosphere.mist.worker

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import scala.concurrent.duration._

class MasterBridgeSpec extends  FunSpec with Matchers with BeforeAndAfterAll {

  it("should create named context with spark.streaming.stopSparkContextByDefault=false") {
    val sparkConf = Map(
      "spark.streaming.stopSparkContextByDefault" -> "true",
      "spark.master" -> "local[*]"
    )
    val namedContext = MasterBridge.createNamedContext("test")(
      WorkerInitInfo(
        sparkConf, 1, 20 seconds, 20 seconds, "localhost:2005", "localhost:2004", 202020)
    )
    val propertyValue = namedContext.sparkContext.getConf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
    propertyValue shouldBe false
    namedContext.sparkContext.stop()
  }
}
