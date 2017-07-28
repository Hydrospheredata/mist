package io.hydrosphere.mist.api.v2

import java.util

import io.hydrosphere.mist.api.v2.hlist._
import org.apache.spark.SparkContext

object Main extends App {

  import JobAsd._
  import Extractor._

  val x = withArgs(
    Arg[Int]("numbers"),
    Arg[String]("marker")
  ).withContext((numbers, marker, spark) => {
    val rdd = spark.parallelize(1 to numbers)
    val r = rdd.map(x => x * 2).collect()
    JobSuccess(r)
  })

  val goodArguments = Map(
    "numbers" -> 10,
    "marker" -> "yoyoyo"
  )
  val badArguments = Map(
    "numbers" -> List("1","2","3", "4", "5"),
    "mult" -> 5
  )

  println(x.extractValues(goodArguments))


  val sparkContext = new SparkContext("local[*]", "app")

  val value = x.run(x.extractValues(goodArguments).get, sparkContext)
  value match {
    case JobFailure(e) => println("ERROR!")
    case JobSuccess(v) => println(s"YOYO ${util.Arrays.toString(v)}")
  }

}

