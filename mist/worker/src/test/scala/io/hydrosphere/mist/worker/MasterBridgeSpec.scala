package io.hydrosphere.mist.worker

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.utils.akka.{ActorF, ActorRegHub}
import org.scalatest.{BeforeAndAfterAll, FunSpec, FunSpecLike, Matchers}

import scala.concurrent.duration._

class MasterBridgeSpec extends TestKit(ActorSystem("WorkerBridgeSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  def mkInitInfo(sparkConf: Map[String, String]) =
    WorkerInitInfo(sparkConf, 1, 20 seconds, 20 seconds, "localhost:2005", "localhost:2003", "localhost:2004", 202020, "")

  it("should create named context with spark.streaming.stopSparkContextByDefault=false") {
    val sparkConf = Map(
      "spark.streaming.stopSparkContextByDefault" -> "true",
      "spark.master" -> "local[*]",
      "spark.driver.allowMultipleContexts" -> "true"
    )
    val namedContext = MasterBridge.createNamedContext("test")( mkInitInfo(sparkConf))
    val propertyValue = namedContext.sparkContext.getConf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
    propertyValue shouldBe false
    namedContext.sparkContext.stop()
  }

  it("should shutdown correctly") {
    val namedMock = mock[NamedContext]
    when(namedMock.getUIAddress()).thenReturn(Some("addr"))

    val regHub = TestProbe()
    val worker = TestProbe()

    val props = MasterBridge.props("id", regHub.ref, _ => namedMock, ActorF.static[(WorkerInitInfo, NamedContext)](worker.ref))
    val bridge = system.actorOf(props)

    regHub.expectMsgType[ActorRegHub.Register]

    val remote = TestProbe()
    remote.send(bridge, mkInitInfo(Map.empty))

    remote.expectMsgType[WorkerReady]
    remote.send(bridge, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, action = Action.Execute)))
    worker.expectMsgType[RunJobRequest]

    remote.send(bridge, ShutdownWorker)
    remote.expectMsgType[RequestTermination.type]
  }
}
