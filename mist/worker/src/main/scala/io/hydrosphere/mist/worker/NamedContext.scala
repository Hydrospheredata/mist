package io.hydrosphere.mist.worker

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
  val loggingConf: Option[CentralLoggingConf] = None,
  streamingDuration: Duration = Duration(40 * 1000)
) {

  private val jars = mutable.Buffer.empty[String]

  def isK8S: Boolean = sparkContext.getConf.get("spark.master").startsWith("k8s://")

  def addJar(artifact: SparkArtifact): Unit = synchronized {
    val path = if (isK8S) artifact.url else artifact.local.getAbsolutePath
    if (!jars.contains(path)) {
      sparkContext.addJar(path)
      jars += path
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

  def getUIAddress(): Option[String] = SparkUtils.getSparkUiAddress(sparkContext)

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

