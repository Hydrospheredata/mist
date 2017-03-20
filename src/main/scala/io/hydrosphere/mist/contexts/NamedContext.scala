package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.lib.spark1.SetupConfiguration
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class NamedContext(
  context: SparkContext,
  namespace: String,
  streamingDuration: Duration,
  publisherConnectionString: String
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
    SetupConfiguration(context, streamingDuration, publisherConnectionString)
  }

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

    val duration = MistConfig.Contexts.streamingDuration(namespace)

    val context = new SparkContext(sparkConf)
    //TODO:!!!
    new NamedContext(context, namespace, duration, "")
  }
}
