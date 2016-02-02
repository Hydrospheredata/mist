package com.provectus.lymph.contexts

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

private[lymph] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  lazy val sqlContext = new SQLContext(context)

  lazy val hiveContext = new HiveContext(context)

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

private[lymph] case class OrdinaryContextWrapper(context: SparkContext) extends ContextWrapper

private[lymph] case class NamedContextWrapper(context: SparkContext, name: String) extends ContextWrapper
