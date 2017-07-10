package io.hydrosphere.mist.worker

import io.hydrosphere.mist.api.CentralLoggingConf
import org.scalatest._

import scala.concurrent.duration._

class WorkerArgumentSpec extends FunSpec with Matchers {

  it("should parse args") {
    val args = Seq(
      "--master", "master:9999",
      "--name", "foo",
      "--context-name", "bar",
      "--spark-conf", "x=y",
      "--mode", "exclusive",
      "--max-jobs", "10",
      "--downtime", "Inf",
      "--spark-streaming-duration", "10s",
      "--log-service", "logs:8888"
    )

    val parsed = WorkerArguments.parse(args).get

    parsed.masterNode shouldBe "akka.tcp://mist@master:9999"
    parsed.name shouldBe "foo"
    parsed.contextName shouldBe "bar"
    parsed.sparkConfParams shouldBe Map("x" -> "y")
    parsed.mode shouldBe "exclusive"
    parsed.downtime shouldBe Duration.Inf
    parsed.streamingDuration shouldBe 10.seconds
    parsed.centralLoggingConf shouldBe Some(CentralLoggingConf("logs", 8888))
  }

  it("should parse many spark-conf args") {
    val args = Seq("--spark-conf", "one=1", "--spark-conf", "two=2")
    val parsed = WorkerArguments.parse(args).get

    parsed.sparkConfParams shouldBe Map("one" -> "1", "two" -> "2")
  }
}
