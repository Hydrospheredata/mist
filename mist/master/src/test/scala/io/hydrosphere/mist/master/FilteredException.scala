package io.hydrosphere.mist.master

class FilteredException extends RuntimeException
object FilteredException {
  def apply(): FilteredException = new FilteredException()
}