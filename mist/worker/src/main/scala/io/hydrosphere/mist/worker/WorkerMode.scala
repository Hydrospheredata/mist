package io.hydrosphere.mist.worker

sealed trait WorkerMode
case object Shared extends WorkerMode
/**
  * Worker only for one job invocation
  */
case object Exclusive extends WorkerMode