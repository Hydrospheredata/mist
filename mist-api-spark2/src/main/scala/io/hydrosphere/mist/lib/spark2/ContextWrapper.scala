package io.hydrosphere.mist.lib.spark2

import java.io.File

import io.hydrosphere.mist.utils.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  private var isHive = false

  lazy val sparkSession: SparkSession = {
     var builder = SparkSession
      .builder()
      .appName(context.appName)
      .config(context.getConf)
     if (isHive) {
       builder = builder.enableHiveSupport()
     }
     builder.getOrCreate()
  }

  def withHive(): ContextWrapper = {
    isHive = true
    this
  }

  def context: SparkContext

  def javaContext: JavaSparkContext = new JavaSparkContext(context)

  def sparkConf: SparkConf = context.getConf

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
