package io.hydrosphere.mist.master.logging

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.testkit.TestKit
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.FilteredException
import org.mockito.Mockito.verify
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration._
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

  it("should ignore errors") {
    val event = LogEvent.mkDebug("id", "message")
    val writer = mock[LogsWriter]
    when(writer.write(any[String], any[Seq[LogEvent]]))
      .thenSuccess(LogUpdate("id", Seq(event), 1))
      .thenFailure(FilteredException())
      .thenSuccess(LogUpdate("id", Seq(event), 1))
      .thenFailure(FilteredException())
      .thenSuccess(LogUpdate("id", Seq(event), 1))

    val in = (1 to 5).map(i => LogEvent.mkDebug(s"job-$i", "message"))
    val future = Source(in)
      .via(LogStreams.storeFlow(writer))
      .take(3)
      .toMat(Sink.seq)(Keep.right).run()


    val updates = Await.result(future, Duration.Inf)
    updates.flatMap(_.events).size shouldBe 3
  }

}
