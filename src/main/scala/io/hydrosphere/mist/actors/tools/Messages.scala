package com.provectus.mist.actors.tools

import com.provectus.mist.contexts.ContextWrapper

private[mist] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: ContextWrapper)
}
