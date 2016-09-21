package io.hydrosphere.mist.contexts

import java.io.File

import io.hydrosphere.mist.MistConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextWrapper {

  private val jars: ArrayBuffer[String] = ArrayBuffer.empty[String]

  lazy val sparkSession = 
     SparkSession
      .builder()
      .appName(context.appName)
      .config(context.getConf)
      .getOrCreate()
  
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

private[mist] case class NamedContextWrapper(context: SparkContext, name: String) extends ContextWrapper{
  
  override lazy val sparkSession = if(MistConfig.Contexts.enableHiveSupport(name)){
     SparkSession
      .builder()
      .appName(context.appName)
      .config(context.getConf)
      .enableHiveSupport()
      .getOrCreate()
  } else {
     SparkSession
      .builder()
      .appName(context.appName)
      .config(context.getConf)
      .getOrCreate()
  }

}
