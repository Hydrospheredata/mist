package mist.api.jdsl

import mist.api.{MistExtras, RawHandle, SparkArgs}
import mist.api.jdsl.FuncSyntax._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.api.java.JavaStreamingContext

trait JJobDefinition extends WithArgs {

  /**
    * Define job execution that use JavaSparkContext for invocation
    */
  def onSparkContext[R](f: Func1[JavaSparkContext, R]): RawHandle[R] = {
    SparkArgs.javaSparkContextArg.apply(f.toScalaFunc)
  }

  /**
    * Define job execution that use JavaStreamingContext for invocation
    */
  def onStreamingContext[R](f: Func1[JavaStreamingContext, R]): RawHandle[R] = {
    SparkArgs.javaStreamingContextArg.apply(f.toScalaFunc)
  }

  /**
    * Define job execution that use SparkSession for invocation
    */
  def onSparkSession[R](f: Func1[SparkSession, R]): RawHandle[R] = {
    SparkArgs.sparkSessionArg.apply(f.toScalaFunc)
  }

  /**
    * Define job execution that use SparkSession with enabled Hive for invocation
    */
  def onSparkSessionWithHive[R](f: Func1[SparkSession, R]): RawHandle[R] = {
    SparkArgs.sparkSessionWithHiveArg.apply(f.toScalaFunc)
  }

  def withMistExtras(): Args1[MistExtras] = new Args1[MistExtras](MistExtras.mistExtras)

}

object JJobDefinition extends WithArgs
