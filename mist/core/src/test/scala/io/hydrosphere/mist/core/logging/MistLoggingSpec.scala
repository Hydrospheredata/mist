package io.hydrosphere.mist.core.logging

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.scalatest.{FunSpec, Matchers}

class MistLoggingSpec extends FunSpec with Matchers {

  describe("levels") {
    it("should restore level from int") {
      Level.fromInt(1) shouldBe Level.Debug
      Level.fromInt(2) shouldBe Level.Info
      Level.fromInt(3) shouldBe Level.Warn
      Level.fromInt(4) shouldBe Level.Error
    }
  }

  describe("log event") {

    it("should have correct format") {
      val ts = LocalDateTime.now(ZoneOffset.UTC)
      val e = LogEvent.mkInfo("job-id", "Message", ts.toInstant(ZoneOffset.UTC).toEpochMilli)

      val expectedDate = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(ts)
      val expected = s"INFO $expectedDate [job-id] Message"

      e.mkString shouldBe expected
    }

    it("should have stack traces") {
      val ts = LocalDateTime.now(ZoneOffset.UTC)
      val error = new RuntimeException("Test error")
      val e = LogEvent.mkError("job-id", "Error", Some(error), ts.toInstant(ZoneOffset.UTC).toEpochMilli)

      val expectedDate = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(ts)
      val expected =
        s"""ERROR $expectedDate [job-id] Error
            |java.lang.RuntimeException: Test error""".stripMargin

      e.mkString should startWith(expected)
    }
  }
}
