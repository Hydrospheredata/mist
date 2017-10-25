package mist.api.jdsl

import mist.api.BaseContexts._
import mist.api.jdsl.FuncSyntax._
import org.apache.spark.api.java.JavaSparkContext

trait JJobDefinition extends WithArgs {

  def onSparkContext[R](f: Func1[JavaSparkContext, RetVal[R]]): JJobDef[R] = {
    val job = javaSparkContext.apply(f.toScalaFunc)
    new JJobDef(job)
  }

}

object JJobDefinition extends WithArgs
