package io.hydrosphere.mist.worker

import akka.actor.ActorSystem
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.SparkConf

object Worker extends App with Logger {

  val name = args(0)
  val contextName = args(1)

  val sparkConf = new SparkConf()
    .setAppName(name)
    .set("spark.driver.allowMultipleContexts", "true")

  val sparkConfSettings = MistConfig.Contexts.sparkConf(contextName)
  for (keyValue <- sparkConfSettings) {
    sparkConf.set(keyValue.head, keyValue(1))
  }
  val context = NamedContext(name, sparkConf)

  val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val downtime = MistConfig.Contexts.downtime(name)
  val maxJobs = MistConfig.Settings.threadNumber

  val props = ClusterWorker.props(
    name = name,
    workerProps = WorkerActor.props(name, context, downtime, maxJobs)
  )
  system.actorOf(props, s"worker-$name")

  val msg =
    s"""Worker $name is started
       |settings:
       |  maxJobs = $maxJobs
       |  downtime = $downtime
       |  sparkConf = ${sparkConf.getAll.mkString(",")}
     """.stripMargin

  logger.info(msg)
}
