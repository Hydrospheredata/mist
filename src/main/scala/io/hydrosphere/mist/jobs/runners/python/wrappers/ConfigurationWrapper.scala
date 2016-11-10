package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.jobs.FullJobConfiguration

private[mist] class ConfigurationWrapper(configuration: FullJobConfiguration) {
  def parameters: Map[String, Any] = {
    configuration.parameters
  }

  def path: String = {
    configuration.path
  }

  def className: String = {
    configuration.className
  }
}
