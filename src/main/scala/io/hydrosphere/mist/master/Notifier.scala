package io.hydrosphere.mist.master

sealed trait JobEvent
case class

trait Notifier {

  def notify()
}
