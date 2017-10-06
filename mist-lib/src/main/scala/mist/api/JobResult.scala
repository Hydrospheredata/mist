package mist.api

sealed trait JobResult[+A]

final case class JobFailure[+A](e: Throwable) extends JobResult[A]
final case class JobSuccess[+A](value: A) extends JobResult[A]

object JobResult {

  def failure[A](e: Throwable): JobResult[A] = JobFailure[A](e)

  def success[A](a: A): JobResult[A] = JobSuccess(a)
}

