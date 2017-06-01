package io.hydrosphere.mist.worker

import java.io.File

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.api.SetupConfiguration
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class NamedContext(
  val context: SparkContext,
  val namespace: String,

  streamingDuration: Duration,
  publisherConnectionString: String,
  publisherTopic: String
) {

  private val jars = mutable.Buffer.empty[String]

  def addJar(jarPath: String): Unit = {
    val jarAbsolutePath = new File(jarPath).getAbsolutePath
    if (!jars.contains(jarAbsolutePath)) {
      context.addJar(jarPath)
      jars += jarAbsolutePath
    }
  }

  def setupConfiguration: SetupConfiguration = {
    SetupConfiguration(
      context = context,
      streamingDuration = streamingDuration,
      publisherConnectionString = publisherConnectionString,
      publisherTopic = publisherTopic
    )
  }

  //TODO: can we call that inside python directly using setupConfiguration?
  // python support
  def sparkConf: SparkConf = context.getConf

  // python support
  def javaContext: JavaSparkContext = new JavaSparkContext(context)

  // python support
  def sqlContext: SQLContext = new SQLContext(context)

  // python support
  def hiveContext: HiveContext = new HiveContext(context)

  def stop(): Unit = {
    context.stop()
  }

}

object NamedContext {

  def apply(namespace: String): NamedContext = {
    val sparkConf = new SparkConf()
      .setAppName(namespace)
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkConfSettings = MistConfig.Contexts.sparkConf(namespace)

    for (keyValue: List[String] <- sparkConfSettings) {
      sparkConf.set(keyValue.head, keyValue(1))
    }
    NamedContext(namespace, sparkConf)
  }

  def apply(namespace: String, sparkConf: SparkConf): NamedContext = {
    val duration = MistConfig.Contexts.streamingDuration(namespace)
    //TODO: if there is no global publisher configuration??
    val publisherConf = globalPublisherConfiguration()
    val publisherTopic = globalPublisherTopic()

    val context = new SparkContext(sparkConf)
    new NamedContext(context, namespace, duration, publisherConf, publisherTopic)
  }

  private def globalPublisherConfiguration(): String = {
    if (MistConfig.Kafka.isOn)
      s"kafka://${MistConfig.Kafka.host}:${MistConfig.Kafka.port}"
    else if (MistConfig.Mqtt.isOn)
      s"mqtt://tcp://${MistConfig.Mqtt.host}:${MistConfig.Mqtt.port}"
    else
      ""
  }

  private def globalPublisherTopic(): String = {
    if (MistConfig.Kafka.isOn)
      MistConfig.Kafka.publishTopic
    else if (MistConfig.Mqtt.isOn)
      MistConfig.Mqtt.publishTopic
    else
      ""
  }
}
