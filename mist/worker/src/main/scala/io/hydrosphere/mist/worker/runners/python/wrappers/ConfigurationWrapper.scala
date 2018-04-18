package io.hydrosphere.mist.worker.runners.python.wrappers

import io.hydrosphere.mist.core.CommonData.JobParams
import io.hydrosphere.mist.utils.Collections
import mist.api.data.JsLikeData

class ConfigurationWrapper(configuration: JobParams) {

  def parameters: java.util.HashMap[String, Any] = Collections.asJavaRecursively(JsLikeData.untyped(configuration.arguments))

  def path: String = configuration.filePath

  def className: String = configuration.className


}
