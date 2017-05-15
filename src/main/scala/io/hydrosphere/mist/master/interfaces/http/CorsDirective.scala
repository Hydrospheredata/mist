package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.{HttpHeader, HttpRequest, StatusCodes}
import akka.http.scaladsl.server._

import scala.collection.immutable._

trait CorsDirective {

  import Directives._
  import StatusCodes._

  val defaultHeaders = Seq(
    `Access-Control-Allow-Origin`.*,
    `Access-Control-Allow-Methods`(Seq(GET, POST, PUT, DELETE, HEAD, OPTIONS))
  )

  def cors(): Directive0 = extractRequest.flatMap(request => {
    val headers = corsHeaders(request)
    if (request.method == OPTIONS) {
      respondWithHeaders(headers) & complete(OK, "Preflight response")
    } else {
      respondWithHeaders(headers) & pass
    }
  })

  private def corsHeaders(request: HttpRequest): Seq[HttpHeader] = {
    val accessed = request.header[`Access-Control-Request-Headers`].map(_.headers).getOrElse(Seq.empty)
    val allowHeaders = `Access-Control-Allow-Headers`(accessed)
    allowHeaders +: defaultHeaders
  }

}

object CorsDirective extends CorsDirective
