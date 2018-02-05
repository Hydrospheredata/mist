package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.MainService
import io.hydrosphere.mist.master.interfaces.JsonCodecs._
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger
import spray.json.{DeserializationException, JsValue, JsonParser}
import spray.json.enrichString

import scala.util.{Failure, Success, Try}

class AsyncInterface(
  mainService: MainService,
  input: AsyncInput,
  val name: String
) extends Logger {

  private val source = Source.Async(name)

  def start(): this.type  = {
    input.start(process)
    this
  }

  private def process(message: String): Unit = {
    def asFunctionRequest(js: JsValue) = js.convertTo[AsyncFunctionStartRequest]
    def asDevRequest(js: JsValue) = js.convertTo[DevJobStartRequestModel]

    try {
      val js = message.parseJson

      Try[Any](asDevRequest(js)).orElse(Try(asFunctionRequest(js))) match {
        case Success(req: AsyncFunctionStartRequest) =>
          mainService.runJob(req.toCommon, source)
        case Success(req: DevJobStartRequestModel) =>
          mainService.devRun(req.toCommon, source)
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

