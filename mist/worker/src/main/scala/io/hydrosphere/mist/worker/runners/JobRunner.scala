package io.hydrosphere.mist.worker.runners

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.MistScContext
import mist.api.data.JsData

import scala.util._

trait JobRunner {

  def run(req: RunJobRequest, context: MistScContext): Either[Throwable, JsData]
}


