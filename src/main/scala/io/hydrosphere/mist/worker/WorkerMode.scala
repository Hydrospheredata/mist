package io.hydrosphere.mist.worker

import scala.concurrent.duration.Duration

sealed trait WorkerMode

/**
  * One worker for multiply jobs
  */
case class Shared(maxJobs:Int, idleTimeout: Duration) extends WorkerMode

/**
  * Worker only for one job invocation
  */
case object Exclusive extends WorkerMode