package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.NamedContext

trait JobRunner {

  def run(
    request: RunJobRequest,
    context: NamedContext
  ): Either[String, Map[String, Any]]

}


