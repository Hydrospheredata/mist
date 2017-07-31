package io.hydrosphere.mist.api.v2

trait Tupler[T] {
  type Out
  def apply(hlist: T): Out
}
