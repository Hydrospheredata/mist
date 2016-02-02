package com.provectus.lymph

/** Base class for repository pattern
  *
  * @tparam T contained type
  */
private[lymph] trait Repository[T] {
  def add(element: T)
  def remove(element: T)

  def get(specification: Specification[T]): Option[T]
  def filter(specification: Specification[T]): List[T]
}
