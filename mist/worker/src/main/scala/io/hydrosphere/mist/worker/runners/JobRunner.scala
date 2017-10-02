package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.NamedContext

import _root_.scala.concurrent.{ExecutionContext, Future}

trait JobRunner {

  def run(
    request: RunJobRequest,
    context: NamedContext
  ): Either[Throwable, Map[String, Any]]

}


