package mist.api

import mist.api.internal.ArgCombiner

/**
  * Access to mist-specific job parameters
  */
class MistExtras(
  val jobId: String,
  val workerId: String
)

object MistExtras {

  val mistExtras: ArgDef[MistExtras] = SystemArg(Seq.empty, ctx => {
    val jobId = ctx.info.id
    val workerId = ctx.info.workerId
    Extracted(new MistExtras(
      jobId = jobId,
      workerId = workerId
    ))
  })
}

/**
  * Get access to mist-extras in job definition
  * Example:
  * {{{
  *   withMistExtras.onSparkContext((extras: MistExtras, sc: SparkContext) => {
  *      val jobId = extras.jobId
  *   })
  * }}}
  */
trait MistExtrasDef {

  import MistExtras._

  implicit class ExtrasOps[A](argDef: ArgDef[A]) {

    def withMistExtras[Out](implicit cmb: ArgCombiner.Aux[A, MistExtras, Out]): ArgDef[Out] =
      cmb(argDef, mistExtras)
  }

  /**
    * Get access to mist-extras in job definition
    */
  def withMistExtras: ArgDef[MistExtras] = mistExtras
}

object MistExtrasDef extends MistExtrasDef
