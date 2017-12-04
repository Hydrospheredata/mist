package mist.api.jdsl

import mist.api.BaseContextsArgs
import mist.api.MistExtras
import mist.api.jdsl.FuncSyntax._
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.api.java.JavaStreamingContext

trait JJobDefinition extends WithArgs {

  /**
    * Define job execution that use JavaSparkContext for invocation
    */
  def onSparkContext[R](f: Func1[JavaSparkContext, RetVal[R]]): JHandle[R] = {
    val job = BaseContextsArgs.javaSparkContext.apply(f.toScalaFunc)
    new JHandle(job)
  }

  /**
    * Define job execution that use JavaStreamingContext for invocation
    */
  def onStreamingContext[R](f: Func1[JavaStreamingContext, RetVal[R]]): JHandle[R] = {
    val job = BaseContextsArgs.javaStreamingContext.apply(f.toScalaFunc)
    new JHandle(job)
  }

  def withMistExtras(): Args1[MistExtras] = new Args1[MistExtras](MistExtras.mistExtras)

}

object JJobDefinition extends WithArgs
