package io.hydrosphere.mist.worker

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.utils.akka.{ActorF, ActorRegHub}
import mist.api.data.JsMap
import org.apache.spark.SparkConf
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
    val namedContext = MistScContext("test", 1 second, new SparkConf().setAll(sparkConf))
    val propertyValue = namedContext.sc.getConf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
    propertyValue shouldBe false
    namedContext.sc.stop()
  }

  it("should shutdown correctly") {
    val namedMock = mock[MistScContext]
    when(namedMock.getUIAddress()).thenReturn(Some("addr"))

    val regHub = TestProbe()
    val worker = TestProbe()

    val props = MasterBridge.props("id", regHub.ref, _ => namedMock, ActorF.static[(WorkerInitInfo, MistScContext)](worker.ref))
    val bridge = TestActorRef(props)

    regHub.expectMsgType[ActorRegHub.Register]

    val remote = TestProbe()
    remote.send(bridge, mkInitInfo(Map.empty))

    remote.expectMsgType[WorkerReady]
    remote.send(bridge, RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute)))
    worker.expectMsgType[RunJobRequest]

    remote.send(bridge, ShutdownWorker)
    remote.expectMsgType[RequestTermination.type]

    remote.send(bridge, ShutdownWorkerApp)
    remote.expectMsgType[Goodbye.type]
  }
}
