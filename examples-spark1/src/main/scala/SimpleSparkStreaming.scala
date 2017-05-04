import io.hydrosphere.mist.api.{MistJob, Publisher, StreamingSupport}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object SimpleSparkStreaming extends MistJob with StreamingSupport with Publisher {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @return result of the job
    */
  def execute(): Map[String, Any] = {
    val ssc = streamingContext
    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.foreachRDD{ (rdd, time) =>
      publisher.publish(Map(
        "time" -> time,
        "length" -> rdd.collect().length,
        "collection" -> rdd.collect().toList.toString
      ).toString())
    }

    ssc.start()

    for (i <- 1 to 50) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(100)
    }
    ssc.stop()
    Map.empty[String, Any]
  }
}


