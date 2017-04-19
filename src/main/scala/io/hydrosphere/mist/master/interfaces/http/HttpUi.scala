package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import akka.http.scaladsl.server.{Directives, RejectionHandler, Route}
/**
  * Server static ui resources
  */
object HttpUi extends Directives {

  import StatusCodes._

  private val notFound = RejectionHandler.newBuilder()
    .handleNotFound(complete(HttpResponse(NotFound, entity = "Not found")))
    .result()

  val route: Route = {
    pathPrefix("ui") {
      get {
        pathEnd {
          redirect("/ui/", PermanentRedirect)
        } ~
        pathSingleSlash {
          getFromResource("web/index.html", Default("web/index.html"), getClass.getClassLoader)
        } ~
        handleRejections(notFound) {
          getFromResourceDirectory("web", getClass.getClassLoader)
        }
      }
    }
  }

}
