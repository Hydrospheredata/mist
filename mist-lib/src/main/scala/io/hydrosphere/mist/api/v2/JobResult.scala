package io.hydrosphere.mist.api.v2

sealed trait JobResult[+A]

final case class JobFailure[+A](e: Throwable) extends JobResult[A]
final case class JobSuccess[+A](value: A) extends JobResult[A]

