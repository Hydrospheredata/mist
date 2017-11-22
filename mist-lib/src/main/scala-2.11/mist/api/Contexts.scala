package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}
import mist.api.BaseContextsArgs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkSessionUtils

object SessionArgs {

  val sparkSession: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, false))
  val sparkSessionWithHive: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, true))
}

trait Contexts extends BaseContexts {

  import SessionArgs._

  implicit class SessionOps[A](args: ArgDef[A]) {

    def onSparkSession[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkSession), f, Seq("sql"))

    def onSparkSessionWithHive[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkSessionWithHive), f, Seq("sql", "hive"))
  }

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession as an argument.
    */
  def onSparkSession[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkSession, F, Out]): JobDef[Out] =
    tjd(sparkSession, f, Seq("sql"))

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession
    * with enabled hive as an argument.
    */
  def onSparkSessionWithHive[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkSession, F, Out]): JobDef[Out] =
    tjd(sparkSessionWithHive, f, Seq("sql", "hive"))
}

object Contexts extends Contexts
