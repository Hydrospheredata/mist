package io.hydrosphere.mist.utils

import org.slf4j.LoggerFactory

trait Logger {
  val logger = LoggerFactory.getLogger(this.getClass)
}
