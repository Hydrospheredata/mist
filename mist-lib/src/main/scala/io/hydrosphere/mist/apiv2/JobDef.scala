package io.hydrosphere.mist.apiv2

import io.hydrosphere.mist.api.v2.JobResult

trait JobDef[R] {
  def invoke(ctx: JobContext): JobResult[R]
}
