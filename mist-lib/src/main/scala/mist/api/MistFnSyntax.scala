package mist.api

import org.apache.spark.{SparkContext, SparkSessionUtils}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext
import SparkArgs._
import mist.api.internal.{ArgCombiner, FnForTuple}
import mist.api.encoding.JsEncoder


/**
  * Provide context combinators to complete job definition, that can take some
  * another arguments + spark computational context.
  * <p>
  * Available contexts:
  * <ul>
  *   <li>org.apache.spark.SparkContext</li>
  *   <li>org.apache.spark.streaming.StreamingContext</li>
  *   <li>org.apache.spark.sql.SQLContext</li>
  *   <li>org.apache.spark.sql.hive.HiveContext</li>
  * </ul>
  *</p>
  *
  * There are two ways how to define job using that standard combinators:
  * <p>
  *   For job which doesn't require any external argument except context
  *   use one of bellow functions:
  *   <ul>
  *     <li>onSparkContext((spark: SparkContext) => {...})</li>
  *     <li>onStreamingContext((ssc: StreamingContext) => {...})</li>
  *     <li>onSqlContext((sqlCtx: SQLContext) => {...})</li>
  *     <li>onHiveContext((hiveCtx: HiveContext) => {...})</li>
  *   </ul>
  *   In case when you have arguments you can call that functions on them
  *   {{{
  *     withArgs(arg[Int]("x") & arg[String]("str")).onSparkContext(
  *       (x: Int, str: String, sc: SparkContext) => {
  *         ...
  *     })
  *   }}}
  * </p>
  */
trait MistFnSyntax {

  implicit class AsHandleOps[A](raw: RawHandle[A]) {
    def asHandle(implicit enc: JsEncoder[A]): Handle = raw.toHandle(enc)
  }


  implicit class ContextsOps[A](args: ArgDef[A]) {

    /**
      * Define job execution function that takes current arguments and org.apache.spark.SparkContext
      */
    def onSparkContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkContext, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(sparkContextArg).apply(f)

    /**
      * Define job execution function that takes current arguments and org.apache.spark.streaming.StreamingContext
      */
    def onStreamingContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, StreamingContext, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(streamingContextArg).apply(f)

    /**
      * Define job execution function that takes current arguments and org.apache.spark.sql.SQLContext
      */
    def onSqlContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SQLContext, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(sqlContextArg).apply(f)

    /**
      * Define job execution function that takes current arguments and org.apache.spark.sql.hive.HiveContext
      */
    def onHiveContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, HiveContext, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(hiveContextArg).apply(f)

    def onSparkSession[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(sparkSessionArg).apply(f)

    def onSparkSessionWithHive[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkSession, Cmb],
      fnT: FnForTuple.Aux[Cmb, F, Out]
    ): RawHandle[Out] = args.combine(sparkSessionWithHiveArg).apply(f)
  }

  /**
    * Define job execution function that takes only org.apache.spark.SparkContext as an argument.
    */
  def onSparkContext[F, Out](f: F)(
    implicit
    fnT: FnForTuple.Aux[SparkContext, F, Out]
  ): RawHandle[Out] = sparkContextArg.apply(f)

  /**
    * Define job execution function that takes only org.apache.spark.streaming.StreamingContext as an argument.
    */
  def onStreamingContext[F, Out](f: F)(implicit fnT: FnForTuple.Aux[StreamingContext, F, Out]): RawHandle[Out] =
    streamingContextArg.apply(f)

  /**
    * Define job execution function that takes only org.apache.spark.sql.SQLContext as an argument.
    */
  def onSqlContext[F, Out](f: F)(implicit fnT: FnForTuple.Aux[SQLContext, F, Out]): RawHandle[Out] =
    sqlContextArg.apply(f)

  /**
    * Define job execution function that takes only org.apache.spark.sql.hive.HiveContext as an argument.
    */
  def onHiveContext[F, Out](f: F)(implicit fnT: FnForTuple.Aux[HiveContext, F, Out]): RawHandle[Out] =
    hiveContextArg.apply(f)

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession as an argument.
    */
  def onSparkSession[F, Out](f: F)( implicit fnT: FnForTuple.Aux[SparkSession, F, Out]): RawHandle[Out] =
    sparkSessionArg.apply(f)

  /**
    * Define job execution function that takes only org.apache.spark.sql.SparkSession
    * with enabled hive as an argument.
    */
  def onSparkSessionWithHive[F, Out](f: F)(implicit fnT: FnForTuple.Aux[SparkSession, F, Out]): RawHandle[Out] =
    sparkSessionWithHiveArg.apply(f)
}

object MistFnSyntax extends MistFnSyntax
