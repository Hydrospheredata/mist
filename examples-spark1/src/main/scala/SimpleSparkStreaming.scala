import io.hydrosphere.mist.api._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object SimpleSparkStreaming extends MistJob with StreamingSupport with Logging {

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

    val logger = getLogger

    reducedStream.foreachRDD{ (rdd, time) =>
      val message = Map(
        "time" -> time,
        "length" -> rdd.collect().length,
        "collection" -> rdd.collect().toList.toString
      ).mkString(",")
      logger.info(message)
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


