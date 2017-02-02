package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.jobs.FullJobConfiguration
import io.hydrosphere.mist.utils.Collections



private[mist] class ConfigurationWrapper(configuration: FullJobConfiguration) {
  def parameters: java.util.HashMap[String, Any] = Collections.asJavaRecursively(configuration.parameters)

  def path: String = configuration.path

  def className: String = configuration.className


}
