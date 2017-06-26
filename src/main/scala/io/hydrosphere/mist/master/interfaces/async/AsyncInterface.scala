package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger
import spray.json.{DeserializationException, JsonParser, pimpString}
import JsonCodecs._

class AsyncInterface(
  masterService: MasterService,
  input: AsyncInput,
  source: Source
) extends Logger {

  def start(): this.type  = {
    input.start(process)
    this
  }

  private def process(message: String): Unit = {
    try {
      val json = message.parseJson
      val request = json.convertTo[AsyncJobStartRequest]
      masterService.runJob(request.toCommon, source)
    } catch {
      case e: DeserializationException =>
        logger.warn(s"Message $message does not conform with start request")
      case e: JsonParser.ParsingException =>
        logger.warn(s"Handled invalid message $message")
      case e: Throwable =>
        logger.error(s"Error occurred during handling message $message", e)
    }
  }

  def close(): Unit = input.close()

}

