package io.hydrosphere.mist.master.interfaces.http

import io.hydrosphere.mist.Messages.StatusMessages.FinishedEvent
import org.scalatest.FunSpec

class JsonCodecsSpec extends FunSpec {

  describe("update status events codec") {

    it("should serialize to json") {
      val event = FinishedEvent("id", 0, Map("1" -> 2))
      val json = UpdateStatusEventCodec.toJson(event)
    }

  }
}
