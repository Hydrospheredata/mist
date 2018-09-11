package io.hydrosphere.mist.worker

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Duration => SDuration}
import org.apache.spark.{SparkConf, SparkContext, SparkSessionUtils}

import scala.collection.mutable
import scala.concurrent.duration.Duration

class MistScContext(
  val sc: SparkContext,
  val namespace: String,
  val streamingDuration: SDuration = SDuration(40 * 1000)
) {

  private val jars = mutable.Buffer.empty[String]

  def isK8S: Boolean = sc.getConf.get("spark.master").startsWith("k8s://")

  def addJar(artifact: SparkArtifact): Unit = synchronized {
    val path = if (isK8S) artifact.url else artifact.local.getAbsolutePath
    if (!jars.contains(path)) {
      sc.addJar(path)
      jars += path
    }
  }

  def getUIAddress(): Option[String] = SparkUtils.getSparkUiAddress(sc)

  //TODO: can we call that inside python directly using setupConfiguration?
  // python support
  def sparkConf: SparkConf = sc.getConf

  // python support
  def javaContext: JavaSparkContext = new JavaSparkContext(sc)

  // python support
  def sqlContext: SQLContext = new SQLContext(sc)

  // python support
  def hiveContext: HiveContext = new HiveContext(sc)

  def sparkSession(enableHive: Boolean): SparkSession = SparkSessionUtils.getOrCreate(sc, enableHive)

  def stop(): Unit = {
    sc.stop()
  }

}

object MistScContext {

  def apply(id: String, streamingDuration: Duration, sparkConf: SparkConf): MistScContext = {
    val upd = sparkConf.clone()
      .setAppName(id)
      .set("spark.streaming.stopSparkContextByDefault", "false")

    val duration = SDuration(streamingDuration.toMillis)
    val sc = new SparkContext(upd)
    new MistScContext(sc, id, duration)
  }

  def apply(id: String, streamingDuration: Duration): MistScContext = apply(id, streamingDuration, new SparkConf())

}

