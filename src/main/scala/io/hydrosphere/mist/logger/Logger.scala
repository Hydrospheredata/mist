package io.hydrosphere.mist

import org.slf4j.Logger
import org.slf4j.LoggerFactory

trait Logger {
  val logger = LoggerFactory.getLogger(this.getClass)
}




