import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable

object StreamingExample extends MistFn with Logging {

  override def handle: Handle = {
    val raw = onStreamingContext((ssc: StreamingContext) => {
      val rddQueue = new mutable.Queue[RDD[Int]]()
      ssc.queueStream(rddQueue)
        .map(x => (x % 10, 1))
        .reduceByKey(_ + _)
        .foreachRDD((rdd, time) => {
           val values = rdd.collect().toList
           val msg = s"time: $time, length: ${values.length}, collection: $values"
           logger.info(msg)
        })

      ssc.start()
      (1 to 50).foreach(_ => {
        rddQueue.synchronized {
          rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)
        }
        Thread.sleep(1000)
      })
      ssc.stop()
    })
    raw.asHandle
  }

}
