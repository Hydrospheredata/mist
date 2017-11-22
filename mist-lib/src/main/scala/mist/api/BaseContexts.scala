package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaStreamingContext

/**
  * Arguments for constructing contexts
  */
object BaseContextsArgs {


  val sparkContext: ArgDef[SparkContext] = SystemArg {
    c => Extracted(c.setupConf.context)
  }

  val streamingContext: ArgDef[StreamingContext] = SystemArg {
    ctx => {
      val conf = ctx.setupConf
      val ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(conf.context, conf.streamingDuration))
      Extracted(ssc)
    }
  }

  val sqlContext: ArgDef[SQLContext] = sparkContext.map(SQLContext.getOrCreate)

  // HiveContext should be cached per jvm
  // see #325
  val hiveContext: ArgDef[HiveContext] = new SystemArg[HiveContext] {

    var cache: HiveContext = _

    override def extract(ctx: JobContext): ArgExtraction[HiveContext] = synchronized {
      ctx match {
        case c: FullJobContext =>
          if (cache == null)
            cache = new HiveContext(c.setupConf.context)
          Extracted(cache)
        case _ =>
          Missing(s"Unknown type of job context ${ctx.getClass.getSimpleName} expected ${FullJobContext.getClass.getSimpleName}")
      }
    }

    override def describe(): Seq[ArgInfo] = Seq(InternalArgument)
  }

  val javaSparkContext: ArgDef[JavaSparkContext] = sparkContext.map(sc => new JavaSparkContext(sc))
  val javaStreamingContext: ArgDef[JavaStreamingContext] = streamingContext.map(scc => new JavaStreamingContext(scc))

}

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
trait BaseContexts {

  import BaseContextsArgs._

  implicit class ContextsOps[A](args: ArgDef[A]) {

    /**
      * Define job execution function that takes current arguments and org.apache.spark.SparkContext
      */
    def onSparkContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SparkContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sparkContext), f)

    /**
      * Define job execution function that takes current arguments and org.apache.spark.streaming.StreamingContext
      */
    def onStreamingContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, StreamingContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(streamingContext), f, Seq("streaming"))

    /**
      * Define job execution function that takes current arguments and org.apache.spark.sql.SQLContext
      */
    def onSqlContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, SQLContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(sqlContext), f, Seq("sql"))

    /**
      * Define job execution function that takes current arguments and org.apache.spark.sql.hive.HiveContext
      */
    def onHiveContext[F, Cmb, Out](f: F)(
      implicit
      cmb: ArgCombiner.Aux[A, HiveContext, Cmb],
      tjd: ToJobDef.Aux[Cmb, F, Out]): JobDef[Out] = tjd(args.combine(hiveContext), f, Seq("hive"))
  }

  /**
    * Define job execution function that takes only org.apache.spark.SparkContext as an argument.
    */
  def onSparkContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SparkContext, F, Out]): JobDef[Out] =
    tjd(sparkContext, f)

  /**
    * Define job execution function that takes only org.apache.spark.streaming.StreamingContext as an argument.
    */
  def onStreamingContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[StreamingContext, F, Out]): JobDef[Out] =
    tjd(streamingContext, f, Seq("streaming"))

  /**
    * Define job execution function that takes only org.apache.spark.sql.SQLContext as an argument.
    */
  def onSqlContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[SQLContext, F, Out]): JobDef[Out] =
    tjd(sqlContext, f, Seq("sql"))

  /**
    * Define job execution function that takes only org.apache.spark.sql.hive.HiveContext as an argument.
    */
  def onHiveContext[F, Out](f: F)(implicit tjd: ToJobDef.Aux[HiveContext, F, Out]): JobDef[Out] =
    tjd(hiveContext, f, Seq("hive"))

}

object BaseContexts extends BaseContexts
