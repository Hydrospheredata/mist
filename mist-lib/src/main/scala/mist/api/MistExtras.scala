package mist.api

import mist.api.args.ArgCombiner

/**
  * Access to mist-specific job parameters + logger
  */
case class MistExtras(
  jobId: String,
  workerId: String,
  logger: MLogger
)

object MistExtras {

  val mistExtras: ArgDef[MistExtras] = ArgDef.create(InternalArgument)(ctx => {
    val jobId = ctx.setupConfiguration.info.id
    val workerId = ctx.setupConfiguration.info.workerId
    val extras = MistExtras(
      jobId = jobId,
      workerId = workerId,
      logger = MLogger(jobId, ctx.setupConfiguration.loggingConf)
    )
    Extracted(extras)
  })

}

/**
  * Get access to mist-extras in job definition
  * Example:
  * {{{
  *   withMistExtras.onSparkContext((extras: MistExtras, sc: SparkContext) => {
  *      val jobId = extras.jobId
  *      extras.logger.info(s"Hello from my job $jobId")
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
