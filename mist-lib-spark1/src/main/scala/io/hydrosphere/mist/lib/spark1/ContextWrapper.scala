package io.hydrosphere.mist.lib.spark1

import java.io.File

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

private [mist] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

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


