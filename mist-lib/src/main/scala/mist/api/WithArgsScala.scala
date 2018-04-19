package mist.api

import mist.api.internal.ArgDefJoiner

/**
  * Scala dsl to start job definition like `withArgs(a,b...n).onSparkContext(..`
  */
trait WithArgsScala {

  def withArgs[A, Out](a: A)(implicit joiner: ArgDefJoiner.Aux[A, Out]): ArgDef[Out] = joiner(a)

}

object WithArgsScala extends WithArgsScala

