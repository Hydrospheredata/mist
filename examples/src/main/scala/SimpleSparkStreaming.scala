import io.hydrosphere.mist.lib.MistJob
import io.hydrosphere.mist.lib.Publisher

import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

object SimpleSparkStreaming extends MistJob with Publisher {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
    override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val ssc = new StreamingContext(context, Seconds(1))

    val rddQueue = new Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()

    publish("test message from stream job")

    reducedStream.foreachRDD{ (rdd, time) =>
      publish("time: " + time)
      publish("lenght: " + rdd.collect().length)
      publish("collection: " + (rdd.collect().toList).toString)
    }

    ssc.start()
    val r = scala.util.Random

    for (i <- 1 to 30) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(100)
    }
    ssc.stop()
    Map("result" -> "success")
  }
}


