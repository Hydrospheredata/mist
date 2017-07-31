import io.hydrosphere.mist.api.v2._

object SimpleContextV2 extends MistJob {

  override def run = withArgs(
     Arg[Int]("numbers").seq,
     Arg[String]("str")
   ).withContext((numbers, str, spark) => {
     val rdd = spark.parallelize(numbers)
     val x = rdd.map(x => x * 15).collect()
     JobSuccess(x)
   })
}

