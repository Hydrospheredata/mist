import io.hydrosphere.mist.api.v2._

object SimpleContextV2 extends MistJob {

  override def run = withArgs(
     Arg[Int]("n"),
     Arg[String]("str")
   ).withContext((n, str, spark) => {
     val rdd = spark.parallelize(0 to n)
     val x = rdd.map(x => x * 15).collect()
     JobSuccess(x)
   })
}
