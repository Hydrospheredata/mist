import io.hydrosphere.mist.lib.spark1.MistJob

object TestError extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @return result exception Test Error
    */
  def execute(): Map[String, Any] = {
    throw new Exception("Test Error")
  }
}