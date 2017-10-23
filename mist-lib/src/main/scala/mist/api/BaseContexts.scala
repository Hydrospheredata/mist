package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext

trait BaseContexts {

  val sparkContext: ArgDef[SparkContext] = ArgDef.create(InternalArgument)(ctx => Extracted(ctx.setupConfiguration.context))
  val streamingContext: ArgDef[StreamingContext] = ArgDef.create(InternalArgument)(ctx => {
    val conf = ctx.setupConfiguration
    Extracted(
      StreamingContext.getActiveOrCreate(() => new StreamingContext(conf.context, conf.streamingDuration))
    )
  })

  val sqlContext: ArgDef[SQLContext] = sparkContext.map(SQLContext.getOrCreate)

  val hiveContext: ArgDef[HiveContext] = new ArgDef[HiveContext] {

    var cache: HiveContext = null

    override def extract(ctx: JobContext): ArgExtraction[HiveContext] = synchronized {
      if (cache == null)
        cache = new HiveContext(ctx.setupConfiguration.context)
      Extracted(cache)
    }

    override def describe(): Seq[ArgInfo] = Seq(InternalArgument)
  }

  val javaSparkContext: ArgDef[JavaSparkContext] = sparkContext.map(sc => new JavaSparkContext(sc))
  val javaStreamingContext: ArgDef[JavaStreamingContext] = streamingContext.map(scc => new JavaStreamingContext(scc))

  implicit class ContextsOps[A](args: ArgDef[A]) {

    /**
      * Define job execution function that takes current arguments and
      * org.apache.spark.SparkContext
      * Example:
      * <pre>
      * {@code
      *   val args = withArgs(arg[Int]("one"), arg[Int]("two")
      *   args.onSparkContext((one: Int, two: Int, spark: SparkContext) => ...)
      * }
      * </pre>
      * @return
      */
    def onSparkContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkContext), f)

    /**
      * Define job execution function that takes current arguments and
      * org.apache.spark.streaming.StreamingContext
      * Example:
      * <pre>
      * {@code
      *   val args = withArgs(arg[Int]("one"), arg[Int]("two")
      *   args.onStreamingContext((one: Int, two: Int, streamingCtx: StreamingContext) => ...)
      * }
      * </pre>
      * @return
      */
    def onStreamingContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, StreamingContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(streamingContext), f)

    /**
      * Define job execution function that takes current arguments and
      * org.apache.spark.sql.SQLContext
      * Example:
      * <pre>
      * {@code
      *
      *  val args = withArgs(arg[Int]("one"), arg[Int]("two")
      *  args.onSqlContext((one: Int, two: Int, sqlCtx: SQLContext) => ...)
      * }
      * </pre>
      * @return
      */
    def onSqlContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SQLContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sqlContext), f)

    /**
      * Define job execution function that takes current arguments and
      * org.apache.spark.sql.hive.HiveContext
      * Example:
      * <pre>
      * {@code
      *
      *  val args = withArgs(arg[Int]("one"), arg[Int]("two")
      *  args.onHiveContext((one: Int, two: Int, hiveCtx: HiveContext) => ...)
      * }
      * </pre>
      * @return
      */
    def onHiveContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, HiveContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(hiveContext), f)
  }

  def onSparkContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkContext, F, Out]): JobDef[Out] =
    tjd(sparkContext, f)

  def onStreamingContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[StreamingContext, F, Out]): JobDef[Out] =
    tjd(streamingContext, f)

  def onSqlContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SQLContext, F, Out]): JobDef[Out] =
    tjd(sqlContext, f)

  def onHiveContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[HiveContext, F, Out]): JobDef[Out] =
    tjd(hiveContext, f)

}

object BaseContexts extends BaseContexts
