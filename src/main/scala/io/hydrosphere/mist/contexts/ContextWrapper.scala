package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  lazy val sqlContext = new SQLContext(context)

  lazy val hiveContext = {
    MistConfig.Hive.hivetest match {
      case true => new TestHiveContext(context)
      case _ => new HiveContext(context)
    }
  }

  def context: SparkContext

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

private[mist] case class OrdinaryContextWrapper(context: SparkContext) extends ContextWrapper

private[mist] case class NamedContextWrapper(context: SparkContext, name: String) extends ContextWrapper
