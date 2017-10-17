package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkSessionUtils

trait Contexts extends BaseContexts {

  val sparkSession: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, false))
  val sparkSessionWithHive: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, true))

  implicit class SessionOps[A](args: ArgDef[A]) {

    def onSparkSession[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkSession), f)

    def onSparkSessionWithHive[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkSessionWithHive), f)
  }

  def onSparkSession[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkSession, F, Out]): JobDef[Out] =
    tjd(sparkSession, f)

  def onSparkSessionWithHive[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkSession, F, Out]): JobDef[Out] =
    tjd(sparkSessionWithHive, f)
}

object Contexts extends Contexts
