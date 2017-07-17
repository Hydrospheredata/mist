package io.hydrosphere.mist.master.data

import io.hydrosphere.mist.master.data.contexts.ContextsStorage
import io.hydrosphere.mist.master.data.endpoints.EndpointsStorage
import io.hydrosphere.mist.master.logging.LogStorageMappings

case class MistData(
  contexts: ContextsStorage,
  endpoints: EndpointsStorage,
  logs: LogStorageMappings
)

