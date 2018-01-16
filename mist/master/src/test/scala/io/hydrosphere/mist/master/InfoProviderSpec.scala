package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.data.ContextsStorage
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._
import scala.concurrent.Future
class InfoProviderSpec extends FunSpec with Matchers with MockitoSugar {

  import TestUtils._

  it("should return info for worker") {
    val contexts = mock[ContextsStorage]
    when(contexts.getOrDefault(any[String]))
      .thenReturn(Future.successful(FooContext))

    val provider = new InfoProvider(
      LogServiceConfig("logHost", 999, ""),
      HttpConfig("localhost", 2004, "ui", 30 seconds),
      contexts,
      "/tmp"
    )

    val info = provider.workerInitInfo("foo").await

    info.downtime shouldBe FooContext.downtime
    info.streamingDuration shouldBe FooContext.streamingDuration
    info.maxJobs shouldBe FooContext.maxJobs
    info.sparkConf shouldBe FooContext.sparkConf
    info.logService shouldBe "logHost:999"
    info.masterHttpConf shouldBe "localhost:2004"
    info.jobsSavePath shouldBe "/tmp"
  }

}
