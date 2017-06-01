package io.hydrosphere.mist.worker

import akka.actor.ActorSystem
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger
import org.apache.spark
import org.apache.spark._

object Worker extends App with Logger {

  try {

    val name = args(0)
    val contextName = args(1)
    val mode = detectMode(name)

    logger.info(s"Try starting on spark: ${spark.SPARK_VERSION}")

    val sparkConf = new SparkConf()
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkConfSettings = MistConfig.Contexts.sparkConf(contextName)
    for (keyValue <- sparkConfSettings) {
      sparkConf.set(keyValue.head, keyValue(1))
    }

    val context = try { NamedContext(name, sparkConf) }
    catch {
      case e: Throwable =>
        throw new RuntimeException("Spark context initialization failed", e)
    }

    val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

    val props = ClusterWorker.props(
      name = name,
      workerProps = WorkerActor.props(mode, context)
    )
    system.actorOf(props, s"worker-$name")

    val msg =
      s"""Worker $name is started
       |settings:
       |  mode = $mode
       |  sparkConf = ${sparkConf.getAll.mkString(",")}
    """.stripMargin

    logger.info(msg)

    system.awaitTermination()
    context.stop()

  } catch {
    case e: Throwable =>
      logger.error("Fatal error", e)
  }

  private def detectMode(name: String): WorkerMode = {
    val modeArg = if (args.length > 2) args(2) else "shared"
    modeArg match {
      case "shared" =>
        val downtime = MistConfig.Contexts.downtime(name)
        val maxJobs = MistConfig.Settings.threadNumber
        Shared(maxJobs, downtime)

      case "exclusive" => Exclusive
      case arg => throw new IllegalArgumentException(s"Unknown worker mode $arg")

    }
  }
}
