package com.provectus.mist

/** Interface for repository pattern
  *
  * @tparam T contained type
  */
private[mist] trait Repository[T] {
  /** Adds element into repository
    *
    * @param element element for add
    */
  def add(element: T)

  /** Removes element from repository
    *
    * @param element element for remove
    */
  def remove(element: T)

  /** Returns ''first'' element in repository satisfied predicate
    *
    * @param specification instance of [[com.provectus.mist.Specification]] with implemented predicate for filtering
    * @return [[scala.Some]] if element is found and [[scala.None]] of not
    */
  def get(specification: Specification[T]): Option[T]

  /** Returns ''all'' elements in repository satisfied predicate
    *
    * @param specification instance of [[com.provectus.mist.Specification]] with implemented predicate for filtering
    * @return filtered [[scala.List]] of elements
    */
  def filter(specification: Specification[T]): List[T]
}
