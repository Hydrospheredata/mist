package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.server.Directives

class WebsocketRoute(
  atPath: String
) {
  import Directives._

  val route = path(atPath)
}
