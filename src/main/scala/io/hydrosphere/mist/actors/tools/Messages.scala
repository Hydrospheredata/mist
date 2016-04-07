package io.hydrosphere.mist.actors.tools

import io.hydrosphere.mist.contexts.ContextWrapper

private[mist] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: ContextWrapper)
}
