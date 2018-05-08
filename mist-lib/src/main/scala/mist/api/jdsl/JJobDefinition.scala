package mist.api.jdsl

import mist.api.SparkArgs
import mist.api.MistExtras
import mist.api.jdsl.FuncSyntax._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.api.java.JavaStreamingContext

trait JJobDefinition extends WithArgs {

  /**
    * Define job execution that use JavaSparkContext for invocation
    */
  def onSparkContext(f: Func1[JavaSparkContext, RetVal]): JHandle = {
    val job = SparkArgs.javaSparkContext.apply(f.toScalaFunc)
    new JHandle(job)
  }

  /**
    * Define job execution that use JavaStreamingContext for invocation
    */
  def onStreamingContext(f: Func1[JavaStreamingContext, RetVal]): JHandle = {
    val job = SparkArgs.javaStreamingContext.apply(f.toScalaFunc)
    new JHandle(job)
  }

  /**
    * Define job execution that use SparkSession for invocation
    */
  def onSparkSession(f: Func1[SparkSession, RetVal]): JHandle = {
    val job = SparkArgs.sparkSession.apply(f.toScalaFunc)
    new JHandle(job)
  }

  /**
    * Define job execution that use SparkSession with enabled Hive for invocation
    */
  def onSparkSessionWithHive(f: Func1[SparkSession, RetVal]): JHandle = {
    val job = SparkArgs.sparkSessionWithHive.apply(f.toScalaFunc)
    new JHandle(job)
  }

  def withMistExtras(): Args1[MistExtras] = new Args1[MistExtras](MistExtras.mistExtras)

}

object JJobDefinition extends WithArgs
