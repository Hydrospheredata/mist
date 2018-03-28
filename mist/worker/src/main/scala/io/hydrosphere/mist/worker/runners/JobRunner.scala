package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.NamedContext
import mist.api.data.JsLikeData

import scala.util._

trait JobRunner {

  def run(req: RunJobRequest, context: NamedContext): Either[Throwable, JsLikeData]
}


