package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger
import spray.json.{JsValue, DeserializationException, JsonParser, pimpString}
import JsonCodecs._

import scala.util.{Failure, Success, Try}

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
    def asEndpointRequest(js: JsValue) = js.convertTo[AsyncEndpointStartRequest]
    def asDevRequest(js: JsValue) = js.convertTo[DevJobStartRequestModel]

    try {
      val js = message.parseJson

      Try[Any](asDevRequest(js)).orElse(Try(asEndpointRequest(js))) match {
        case Success(req: AsyncEndpointStartRequest) =>
          masterService.runJob(req.toCommon, source)
        case Success(req: DevJobStartRequestModel) =>
          masterService.devRun(req.toCommon, source)
        case Success(x) => throw new IllegalArgumentException("Unknown request type")
        case Failure(e) => throw e
      }
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

