package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.worker.NamedContext

trait JobRunner {

  def run(
    params: JobParams,
    context: NamedContext
  ): Either[String, Map[String, Any]]

}


