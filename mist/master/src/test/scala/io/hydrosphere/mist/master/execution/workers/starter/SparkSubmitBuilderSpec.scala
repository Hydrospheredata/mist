package io.hydrosphere.mist.master.execution.workers.starter

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.TestData
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.duration._

class SparkSubmitBuilderSpec extends FunSpec with Matchers {

  val builder = new SparkSubmitBuilder("/usr/share/mist", "/usr/share/spark")

  val testInfo = WorkerInitInfo(
    sparkConf = Map("spark.master" -> "spark://localhost:4433", "a.b.c" -> "xyz"),
    maxJobs = 10,
    downtime = 1 second,
    streamingDuration = 1 second,
    logService = "localhost:2005",
    masterAddress = "localhost:2003",
    masterHttpConf = "localhost:2004",
    maxArtifactSize = 100L,
    runOptions = ""
  )

  it("should build for local jar") {
    val cmd = builder.submitWorker("name", testInfo)
    cmd should contain theSameElementsInOrderAs Seq(
      "/usr/share/spark/bin/spark-submit",
      "--conf", "spark.master=spark://localhost:4433",
      "--conf", "a.b.c=xyz",
      "--class", "io.hydrosphere.mist.worker.Worker",
      "/usr/share/mist/mist-worker.jar",
      "--master", testInfo.masterAddress,
      "--name", "name"
    )
  }

  it("should build for k8s") {
    val k8s = testInfo.copy(sparkConf = Map("spark.master" -> "k8s://http://localhost:2567"))
    val cmd = builder.submitWorker("name", k8s)
    cmd should contain theSameElementsInOrderAs Seq(
      "/usr/share/spark/bin/spark-submit",
      "--conf", "spark.master=k8s://http://localhost:2567",
      "--class", "io.hydrosphere.mist.worker.Worker",
      s"http://${k8s.masterHttpConf}/v2/api/artifacts_internal/mist-worker.jar",
      "--master", testInfo.masterAddress,
      "--name", "name"
    )
  }

  it("should correctly parse runOptions") {
    val debugCtx = testInfo.copy(runOptions = "--driver-java-options '-Xdebug -Xrunjdwp:transport=dt_socket,address=15000,server=y,suspend=y'")
    val cmd = builder.submitWorker("name", debugCtx)
    cmd should contain theSameElementsInOrderAs Seq(
      "/usr/share/spark/bin/spark-submit",
      "--driver-java-options", "-Xdebug -Xrunjdwp:transport=dt_socket,address=15000,server=y,suspend=y",
      "--conf", "spark.master=spark://localhost:4433",
      "--conf", "a.b.c=xyz",
      "--class", "io.hydrosphere.mist.worker.Worker",
      "/usr/share/mist/mist-worker.jar",
      "--master", testInfo.masterAddress,
      "--name", "name"
    )
  }
}
