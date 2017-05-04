package io.hydrosphere.mist.master.interfaces.async2

import scala.concurrent.Future

trait AsyncClient {

  def publish(event: String): Unit

  def subscribe(f: String => Unit): Unit

  def close(): Future[Unit]

}
