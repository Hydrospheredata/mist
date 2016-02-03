package com.provectus.lymph

/** Interface for specification pattern
  *
  * @tparam T type of repository element
  */
private[lymph] trait Specification[T] {
  /** Predicate for repository filtering
    *
    * @param element repository element for checking
    * @return is this `element` satisfies implemented condition
    */
  def specified(element: T): Boolean
}
