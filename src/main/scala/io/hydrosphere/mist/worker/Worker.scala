package io.hydrosphere.mist.worker

import java.io.File

import akka.actor.ActorSystem
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{ContextConfig, WorkerConfig}
import org.apache.spark
import org.apache.spark._

object Worker extends App with Logger {

  try {

    val name = args(0)
    val contextName = args(1)

    val config = WorkerConfig.load(new File("configs/default.conf"))
    val contextSettings = config.contextsSettings.configFor(contextName)

    val mode = detectMode(contextSettings, name)

    logger.info(s"Try starting on spark: ${spark.SPARK_VERSION}")

    val sparkConf = new SparkConf()
      .setAppName(name)
      .set("spark.driver.allowMultipleContexts", "true")

    for (keyValue <- contextSettings.sparkConf) {
      sparkConf.set(keyValue._1, keyValue._2)
    }

    val context = try { NamedContext(name, contextName, config) }
    catch {
      case e: Throwable =>
        throw new RuntimeException("Spark context initialization failed", e)
    }

    //TODO
    val system = ActorSystem("mist")

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
    logger.info(s"Shutdown worker appilication $name $contextName")
    context.stop()

  } catch {
    case e: Throwable =>
      logger.error("Fatal error", e)
  }

  private def detectMode(config: ContextConfig, name: String): WorkerMode = {
    val modeArg = if (args.length > 2) args(2) else "shared"
    modeArg match {
      case "shared" =>
        val downtime = config.downtime
        val maxJobs = config.maxJobs
        Shared(maxJobs, downtime)

      case "exclusive" => Exclusive
      case arg => throw new IllegalArgumentException(s"Unknown worker mode $arg")

    }
  }
}
