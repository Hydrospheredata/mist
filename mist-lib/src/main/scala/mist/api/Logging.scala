package mist.api

import org.slf4j.LoggerFactory

trait Logging {
  @transient
  lazy val logger = LoggerFactory.getLogger(this.getClass)
}
