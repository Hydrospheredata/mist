package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.Messages.JobMessages.{RunJobRequest, JobParams}
import io.hydrosphere.mist.worker.NamedContext

trait JobRunner {

  def run(
    request: RunJobRequest,
    context: NamedContext
  ): Either[String, Map[String, Any]]

}


