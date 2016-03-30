import com.provectus.mist.MistJob
import org.apache.spark.SparkContext

object noDoStuffMethodError extends MistJob {
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???
}