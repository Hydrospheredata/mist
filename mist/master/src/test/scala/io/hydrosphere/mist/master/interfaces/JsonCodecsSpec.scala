package io.hydrosphere.mist.master.interfaces

import io.hydrosphere.mist.master.Messages.StatusMessages._
import org.scalatest._
import mist.api.data._
import io.hydrosphere.mist.master.TestData
import org.scalatest.prop.TableDrivenPropertyChecks._

import spray.json._

class JsonCodecsSpec extends FunSpec with Matchers  with TestData {

  it("should serialize status events") {
    import JsonCodecs._
    val expected = Table(
      ("event", "name"),
      (QueuedEvent("id"), "queued"),
      (StartedEvent("id", 1), "started"),
      (CanceledEvent("id", 1), "canceled"),
      (FinishedEvent("id", 1, JsLikeMap("1" -> JsLikeNumber(2))), "finished"),
      (FailedEvent("id", 1, "error"), "failed"),
      (WorkerAssigned("id", "workerId"), "worker-assigned"),
      (JobFileDownloadingEvent("id", System.currentTimeMillis()), "job-file-downloading"),
      (KeepAliveEvent, "keep-alive")
    )

     forAll(expected) { (e: SystemEvent, name: String) =>
       val json = e.toJson
       json.asJsObject.fields.get("event") shouldBe Some(JsString(name))
     }
  }
}
