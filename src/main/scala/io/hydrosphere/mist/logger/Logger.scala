package io.hydrosphere.mist

import org.slf4j.Logger
import org.slf4j.LoggerFactory

trait Logger {
  //SLF4JBridgeHandler.removeHandlersForRootLogger()
  //SLF4JBridgeHandler.install()
  val logger = LoggerFactory.getLogger(this.getClass)
}




