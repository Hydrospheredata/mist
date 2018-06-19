package io.hydrosphere.mist.master.interfaces

import io.hydrosphere.mist.master.Messages.StatusMessages._
import org.scalatest._
import mist.api.data._
import io.hydrosphere.mist.master.TestData
import org.scalatest.prop.TableDrivenPropertyChecks._

import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._
import spray.json._

class JsonCodecsSpec extends FunSpec with Matchers  with TestData {

  it("should serialize status events") {
    import JsonCodecs._
    val expected = Table(
      ("event", "name"),
      (QueuedEvent("id"), "queued"),
      (StartedEvent("id", 1), "started"),
      (CancellingEvent("id", 1), "cancelling"),
      (CancelledEvent("id", 1), "cancelled"),
      (FinishedEvent("id", 1, JsMap("1" -> 2.js)), "finished"),
      (FailedEvent("id", 1, "error"), "failed"),
      (WorkerAssigned("id", "workerId"), "worker-assigned"),
      (JobFileDownloadingEvent("id", System.currentTimeMillis()), "job-file-downloading"),
      (KeepAliveEvent, "keep-alive")
    )

     forAll(expected) { (e: SystemEvent, name: String) =>
       val json = e.toJson
       json.asJsObject.fields.get("event") shouldBe Some(spray.json.JsString(name))
     }
  }
}
