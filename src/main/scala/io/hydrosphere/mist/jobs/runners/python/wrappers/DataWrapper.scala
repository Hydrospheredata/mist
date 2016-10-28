package io.hydrosphere.mist.jobs.runners.python.wrappers

private[mist] class DataWrapper {
  private var data: Any = _

  def set(in: Any): Unit = {
    data = in
  }
  def get: Any = data
}
