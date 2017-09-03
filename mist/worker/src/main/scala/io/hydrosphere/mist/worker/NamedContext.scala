package io.hydrosphere.mist.worker

import java.io.File

import io.hydrosphere.mist.api.{CentralLoggingConf, RuntimeJobInfo, SetupConfiguration}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

class NamedContext(
  val sparkContext: SparkContext,
  val namespace: String,
  streamingDuration: Duration = Duration(40 * 1000),
  loggingConf: Option[CentralLoggingConf] = None
) {

  private val jars = mutable.Buffer.empty[String]

  def addJar(jarPath: String): Unit = {
    val jarAbsolutePath = new File(jarPath).getAbsolutePath
    if (!jars.contains(jarAbsolutePath)) {
      sparkContext.addJar(jarPath)
      jars += jarAbsolutePath
    }
  }

  def setupConfiguration(jobId: String): SetupConfiguration = {
    SetupConfiguration(
      context = sparkContext,
      streamingDuration = streamingDuration,
      info = RuntimeJobInfo(jobId, namespace),
      loggingConf = loggingConf
    )
  }

  //TODO: can we call that inside python directly using setupConfiguration?
  // python support
  def sparkConf: SparkConf = sparkContext.getConf

  // python support
  def javaContext: JavaSparkContext = new JavaSparkContext(sparkContext)

  // python support
  def sqlContext: SQLContext = new SQLContext(sparkContext)

  // python support
  def hiveContext: HiveContext = new HiveContext(sparkContext)

  def stop(): Unit = {
    sparkContext.stop()
  }

}

