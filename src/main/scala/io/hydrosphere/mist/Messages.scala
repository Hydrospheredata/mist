package io.hydrosphere.mist

private[mist] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: String)

  case class WorkerDidStart(name: String, address: String)
}
