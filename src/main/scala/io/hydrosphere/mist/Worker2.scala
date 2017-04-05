package io.hydrosphere.mist

import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.master.namespace.RemoteWorker
import akka.actor.ActorSystem
import org.apache.spark.SparkConf

object Worker2 extends App {
  val name = args(0)

  val sparkConf = new SparkConf()
    .setAppName(name)
    .set("spark.driver.allowMultipleContexts", "true")

  val sparkConfSettings = MistConfig.Contexts.sparkConf(name)
  for (keyValue <- sparkConfSettings) {
    sparkConf.set(keyValue.head, keyValue(1))
  }
  val context = NamedContext(name, sparkConf)

  val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)
  val props = RemoteWorker.props(name, context)
  system.actorOf(props, s"worker-$name")

}
