package io.hydrosphere.mist.jobs.runners.jar

import io.hydrosphere.mist.api._

object InvalidJob extends MistJob

object MultiplyJob extends MistJob {

  def execute(numbers: List[Int]): Map[String, Any] = {
    val rdd = context.parallelize(numbers)
    Map("r" -> rdd.map(x => x * 2).collect().toList)
  }
}

object OptParamJob extends MistJob {

  def execute(p: Option[Int]): Map[String, Any] = {
    Map("r" -> p.getOrElse(42))
  }
}

