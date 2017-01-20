package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextWrapper extends Logger {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  lazy val sqlContext = new SQLContext(context)

  lazy val hiveContext: SQLContext = {
    if (MistConfig.Hive.hivetest) {
      new TestHiveContext(context)
    } else {
      new HiveContext(context)
    }
  }

  def context: SparkContext

  def javaContext: JavaSparkContext = new JavaSparkContext(context)

  def sparkConf: SparkConf = context.getConf

  def addJar(jarPath: String): Unit = {
    val jarAbsolutePath = new File(jarPath).getAbsolutePath
    if (!jars.contains(jarAbsolutePath)) {
      logger.info(s"Adding new Jar: $jarAbsolutePath")
      context.addJar(jarPath)
      jars += jarAbsolutePath
    }
  }

  def stop(): Unit = {
    context.stop()
  }
}


