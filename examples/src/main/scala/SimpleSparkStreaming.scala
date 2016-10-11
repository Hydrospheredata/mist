import io.hydrosphere.mist.lib.StreamingMistJob

import scala.collection.mutable.Queue
import org.apache.spark.rdd.RDD

object SimpleSparkStreaming extends StreamingMistJob {

  override def doStuff = {

    val ssc = streamingContext

    val rddQueue = new Queue[RDD[Int]]()

    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()

    publishToMqtt("test message from stream job")

    reducedStream.foreachRDD{ (rdd, time) =>
      publishToMqtt("time: " + time)
      publishToMqtt("lenght: " + rdd.collect().length)
      publishToMqtt("collection: " + (rdd.collect().toList).toString)
    }

    ssc.start()
    val r = scala.util.Random

    for (i <- 1 to 1000) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
      }
      Thread.sleep(100)
    }
    ssc.stop()
  }
}


