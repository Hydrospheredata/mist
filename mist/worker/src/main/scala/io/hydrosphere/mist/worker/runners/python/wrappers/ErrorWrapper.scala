package io.hydrosphere.mist.worker.runners.python.wrappers

class ErrorWrapper {
  private var error: String = ""

  def set(in: String): Unit = {
    error = in
  }
  
  def get(): String =error
}
