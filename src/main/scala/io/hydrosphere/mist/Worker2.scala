package io.hydrosphere.mist

import akka.actor.ActorSystem
import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.master.namespace.{ClusterWorker, WorkerActor}
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.SparkConf

object Worker2 extends App with Logger {

  val namespace = args(0)

  val sparkConf = new SparkConf()
    .setAppName(namespace)
    .set("spark.driver.allowMultipleContexts", "true")

  val sparkConfSettings = MistConfig.Contexts.sparkConf(namespace)
  for (keyValue <- sparkConfSettings) {
    sparkConf.set(keyValue.head, keyValue(1))
  }
  val context = NamedContext(namespace, sparkConf)

  val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val downtime = MistConfig.Contexts.downtime(namespace)
  val maxJobs = MistConfig.Settings.threadNumber

  val props = ClusterWorker.props(
    name = namespace,
    workerProps = WorkerActor.props(namespace, context, downtime, maxJobs)
  )
  system.actorOf(props, s"worker-$namespace")

  val msg =
    s"""Worker $namespace is started
       |settings:
       |  maxJobs = $maxJobs
       |  downtime = $downtime
       |  sparkConf = ${sparkConf.getAll.mkString(",")}
     """.stripMargin

  logger.info(msg)
}
