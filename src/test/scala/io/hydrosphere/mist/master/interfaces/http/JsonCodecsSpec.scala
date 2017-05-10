package io.hydrosphere.mist.master.interfaces.http

import io.hydrosphere.mist.Messages.StatusMessages.{FinishedEvent, UpdateStatusEvent}
import org.scalatest.FunSpec

class JsonCodecsSpec extends FunSpec {

  describe("update status events codec") {

    it("should serialize to json") {
      import spray.json._
      import JsonCodecs._

      val event = FinishedEvent("id", 0, Map("1" -> 2))
      val json = event.asInstanceOf[UpdateStatusEvent].toJson
      println(json)

      println(json.convertTo[UpdateStatusEvent])
    }

  }
}
