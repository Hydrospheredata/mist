package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class NamedContext(
  context: SparkContext,
  namespace: String
) {

  private val jars = mutable.Buffer.empty[String]

  def addJar(jarPath: String): Unit = {
    val jarAbsolutePath = new File(jarPath).getAbsolutePath
    if (!jars.contains(jarAbsolutePath)) {
      context.addJar(jarPath)
      jars += jarAbsolutePath
    }
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
    val context = new SparkContext(sparkConf)
    new NamedContext(context, namespace)
  }
}
