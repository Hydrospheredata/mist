package io.hydrosphere.mist.jobs.jar

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

object ManyArgJob extends MistJob {

  def execute(
    FromDate: String,
    ToDate: String,
    query: String,
    rows: Int,
    Separator: String
  ): Map[String, Any] = {

    val expected = Seq("FromDate", "ToDate", "query", 1, "Separator")
    val given = Seq(FromDate, ToDate, query, rows, Separator)

    val isOk = expected == given
    Map("isOk" -> isOk)
  }
}

