import SimpleSparkStreaming.{context, publish}
import io.hydrosphere.mist.lib.{MQTTPublisher, MistJob}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable

object StreamingTextSearch extends MistJob with MQTTPublisher {
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    context.setLogLevel("INFO")

    val filter: String = parameters("filter").asInstanceOf[String]

    val ssc = new StreamingContext(context, Seconds(1))

    val rddQueue = new mutable.Queue[RDD[String]]()

    val inputStream = ssc.queueStream(rddQueue)

    val filtredStream = inputStream.filter(x => x.toUpperCase.contains(filter.toUpperCase))

    filtredStream.foreachRDD{ (rdd, time) =>
      publish(Map(
        "time" -> time,
        "length" -> rdd.collect().length,
        "collection" -> rdd.collect().toList.toString
      ))
    }

    ssc.start()

    for (i <- 1 to 1000) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD( Seq("test message", "[error] message", "[warning] test message", "[info] test message"), 10)
      }
      Thread.sleep(500)
    }
    ssc.stop()
    Map.empty[String, Any]
  }
}