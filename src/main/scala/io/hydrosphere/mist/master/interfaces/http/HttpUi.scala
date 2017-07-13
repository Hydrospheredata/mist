package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import akka.http.scaladsl.server.{Directives, RejectionHandler, Route}

/**
  * Server static ui resources
  */
class HttpUi(path: String) extends Directives {

  import StatusCodes._

  val index = path + "/index.html"

  private val notFound = RejectionHandler.newBuilder()
    .handleNotFound(getFromFile(index))
    .result()

  val route: Route = {
    pathPrefix("ui") {
      get {
        pathEnd {
          redirect("/ui/", PermanentRedirect)
        } ~
        pathSingleSlash {
          getFromFile(index)
        } ~
        handleRejections(notFound) {
          getFromDirectory(path)
        }
      }
    }
  }

}
