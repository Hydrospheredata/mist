import io.hydrosphere.mist.api.{MistJob2, MistLogging2, SQLSupport2}

object SimpleContext extends MistJob2 with SQLSupport2 with MistLogging2 {

  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param numbers list of int to process
    * @return result of the job
    */
  def execute(numbers: List[Int], multiplier: Option[Int]): Map[String, Any] = {
    val multiplierValue = multiplier.getOrElse(2)
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val df = context.parallelize(numbers).toDF()
    logger.info("HELLLO")
    val x = df.map(x => { val z = x.getInt(0) * multiplierValue; logger.info("SDHSDH"); z }).collect()
    Map("result" -> x)
  }
}
