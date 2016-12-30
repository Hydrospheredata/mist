package io.hydrosphere.mist.utils

import org.slf4j.{LoggerFactory, Logger => SLFLogger}

trait Logger {
  val logger: SLFLogger = LoggerFactory.getLogger(this.getClass)
}
