import io.hydrosphere.mist.lib.MistJob

object TestError extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @return result exception Test Error
    */
  def doStuff(): Map[String, Any] = {
    throw new Exception("Test Error")
  }
}