package io.hydrosphere.mist.jobs.runners.python.wrappers

private[mist] class ErrorWrapper {
  private var error: String = ""

  def set(in: String): Unit = {
    error = in
  }
  def get(): String =error
}
