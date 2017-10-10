package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext

trait BaseContexts {

  val sparkContext: ArgDef[SparkContext] = ArgDef.create(ctx => Extracted(ctx.setupConfiguration.context))
  val steamingContext: ArgDef[StreamingContext] = ArgDef.create(ctx => {
    val conf = ctx.setupConfiguration
    Extracted(
      StreamingContext.getActiveOrCreate(() => new StreamingContext(conf.context, conf.streamingDuration))
    )
  })

  val sqlContext: ArgDef[SQLContext] = sparkContext.map(SQLContext.getOrCreate)
  val hiveContext: ArgDef[HiveContext] = sparkContext.map(sc => new HiveContext(sc))
  val javaSparkContext: ArgDef[JavaSparkContext] = sparkContext.map(sc => new JavaSparkContext(sc))
  val javaStreamingContext: ArgDef[JavaStreamingContext] = steamingContext.map(scc => new JavaStreamingContext(scc))

  implicit class ContextsOps[A](args: ArgDef[A]) {

//    def onSparkContext[B, F, R, R2](f: F)(
//      implicit
//        cmb: ArgCombiner.Aux[A, SparkContext, B],
//        toJobDef: ArgToJobDef.Aux[B, F, R]
//        ): JobDef[R] = toJobDef(cmb(args, sparkContext), f)
//
//
//    def onStreamingContext[B, F, R](f: F)(
//      implicit
//      cmb: ArgCombiner.Aux[A, StreamingContext, B],
//      toJobDef: ArgToJobDef.Aux[B, F, R]): JobDef[R] = toJobDef(cmb(args, steamingContext), f)
//
//    def onSqlContext[B, F, R](f: F)(
//      implicit
//      cmb: ArgCombiner.Aux[A, SQLContext, B],
//      toJobDef: ArgToJobDef.Aux[B, F, R]): JobDef[R] = toJobDef(cmb(args, sqlContext), f)
//
//    def onHiveContext[B, F, R](f: F)(
//      implicit
//      cmb: ArgCombiner.Aux[A, HiveContext, B],
//      toJobDef: ArgToJobDef.Aux[B, F, R]): JobDef[R] = toJobDef(cmb(args, hiveContext), f)

    def onSparkContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkContext), f)
  }

  def onSparkContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkContext, F, Out]): JobDef[Out] =
    tjd(sparkContext, f)
}

object BaseContexts extends BaseContexts
