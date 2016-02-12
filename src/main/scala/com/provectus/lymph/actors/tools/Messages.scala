package com.provectus.lymph.actors.tools

import com.provectus.lymph.contexts.ContextWrapper

private[lymph] object Messages {

  case class CreateContext(name: String)

  case class StopAllContexts()

  case class RemoveContext(context: ContextWrapper)
}
