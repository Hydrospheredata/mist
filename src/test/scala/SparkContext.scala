import com.provectus.mist.MistJob
import org.apache.spark.SparkContext

object SparkContext extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
    val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
    val rdd = context.parallelize(numbers)
    Map("result" -> rdd.map(x => x * 2).collect())
  }
}