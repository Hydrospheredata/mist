package io.hydrosphere.mist.worker

import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.worker.logging.LogsWriter
import mist.api.data.JsMap
import org.apache.log4j.LogManager
import org.scalatest.{FunSpec, Matchers}

class RequestSetupSpec extends FunSpec with Matchers {

  it("should add/remove appender") {
    val logger = LogManager.getLogger("test")
    val setup = RequestSetup.loggingSetup(logger, new LogsWriter {
      override def close(): Unit = ()
      override def write(e: LogEvent): Unit = ()
    })

    val req = RunJobRequest("id", JobParams("path", "MyClass", JsMap.empty, action = Action.Execute))
    val cleanUp = setup(req)
    logger.getAppender("id") should not be null
    cleanUp(req)
    logger.getAppender("id") shouldBe null
  }
}
