package mist.api

import mist.api.args.{ArgCombiner, ArgDef, SystemArg, ToHandle, ArgInfo}
import mist.api.BaseContextsArgs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkSessionUtils

object SessionArgs {

  val sparkSession: ArgDef[SparkSession] = SystemArg(Seq(ArgInfo.SqlContextTag),
    ctx => sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, false)).extract(ctx)
  )

  val sparkSessionWithHive: ArgDef[SparkSession] = SystemArg(
    Seq(ArgInfo.SqlContextTag, ArgInfo.HiveContextTag),
    ctx => sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, true)).extract(ctx))
}

trait Contexts extends BaseContexts {

  import SessionArgs._

  implicit class SessionOps[A](args: ArgDef[A]) {

    def onSparkSession[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToHandle.Aux[Cmb, F, Out]): Handle[Out] = tjd(args.combine(sparkSession), f)

    def onSparkSessionWithHive[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToHandle.Aux[Cmb, F, Out]): Handle[Out] = tjd(args.combine(sparkSessionWithHive), f)
  }

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession as an argument.
    */
  def onSparkSession[F, Out](f: F)(implicit tjd: ToHandle.Aux[SparkSession, F, Out]): Handle[Out] =
    tjd(sparkSession, f)

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession
    * with enabled hive as an argument.
    */
  def onSparkSessionWithHive[F, Out](f: F)(implicit tjd: ToHandle.Aux[SparkSession, F, Out]): Handle[Out] =
    tjd(sparkSessionWithHive, f)
}

object Contexts extends Contexts
