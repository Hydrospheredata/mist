package io.hydrosphere.mist.master.logging

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.core.MockitoSugar
import org.mockito.Mockito.verify
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class LogStreamsSpec extends TestKit(ActorSystem("log-service-test"))
  with FunSpecLike
  with MockitoSugar
  with Matchers {

  implicit val materializer = ActorMaterializer()

  it("should store events") {
    val writer = mock[LogsWriter]
    when(writer.write(any[String], any[Seq[LogEvent]]))
      .thenReturn(Future.successful(LogUpdate("jobId", Seq.empty, 1)))


    val out = Source.single(LogEvent.mkDebug("id", "message"))
      .via(LogStreams.storeFlow(writer))
      .take(1)
      .toMat(Sink.seq)(Keep.right).run()

    val updates = Await.result(out, Duration.Inf)

    updates.size shouldBe 1
    verify(writer).write(any[String], any[Seq[LogEvent]])
  }
}
