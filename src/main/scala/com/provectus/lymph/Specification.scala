package com.provectus.lymph

/** Base class for specification pattern
  *
  * @tparam T contained type
  */
private[lymph] trait Specification[T] {
  def specified(context: T): Boolean
}
