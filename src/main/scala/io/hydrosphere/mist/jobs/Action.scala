package io.hydrosphere.mist.jobs

sealed trait Action
object Action {

  def apply(string: String): Action = string match {
    case "execute" => Execute
    case "serve" => Serve
  }

  case object Execute extends Action {
    override def toString: String = "execute"
  }

  case object Serve extends Action {
    override def toString: String = "serve"
  }
}

