package io.hydrosphere.mist.worker

import io.hydrosphere.mist.api.CentralLoggingConf
import akka.actor.ActorSystem
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.NetUtils

import org.apache.spark._

import scala.concurrent.duration._


case class WorkerArguments(
  bindAddress: String = "localhost:0",
  masterAddress: String = "",
  name: String = "",
  contextName: String = "",
  sparkConfString: String = "",
  maxJobs: Int = -1,
  downtime: Duration = Duration.Inf,
  mode: String = "shared",
  streamingDuration: Duration = 40 seconds,
  logServiceAddress: Option[String] = None
) {

  def sparkConf: SparkConf = {
    val params = sparkConfString.split(",").map(x => {
      val pair = x.split("=")
      pair(0) -> pair(1)
    })
    new SparkConf().setAppName(name).setAll(params)
  }

  def masterNode: String = s"akka.tcp://mist@$masterAddress"

  def workerMode: WorkerMode = mode match {
    case "shared" => Shared(maxJobs, downtime)
    case "exclusive" => Exclusive
    case arg => throw new IllegalArgumentException(s"Unknown worker mode $arg")
  }

  def centralLoggingConf: Option[CentralLoggingConf] = logServiceAddress.map(s => {
      val pair = s.split(":")
      CentralLoggingConf(pair(0), pair(1).toInt)
  })

  def bindHost: String = bindAddress.split(":")(0)
  def bindPort: Int = bindAddress.split(":")(1).toInt

  def createNamedContext: NamedContext = {
    val sparkContext = new SparkContext(sparkConf)
    new NamedContext(
      sparkContext,
      contextName,
      org.apache.spark.streaming.Duration(streamingDuration.toMillis),
      centralLoggingConf
    )
  }
}


object WorkerArguments {

  val parser = new scopt.OptionParser[WorkerArguments]("mist-worker") {

    override def errorOnUnknownArgument: Boolean = false
    override def reportWarning(msg: String): Unit = {}

    head("mist-worker")

    opt[String]("bind-address").action((x, a) => a)

    opt[String]("master").action((x, a) => a.copy(masterAddress = x))
      .text("host:port to master")

    opt[String]("name").action((x, a) => a.copy(name = x))
      .text("Uniq name of worker")

    opt[String]("context-name").action((x, a) => a.copy(contextName = x))
      .text("Mist context name")

    opt[String]("spark-conf").action((x,a) => a.copy(sparkConfString = x))
      .text("Spark context configuration")

    opt[String]("mode").action((x, a) => a.copy(mode = x))
      .validate({
        case "exclusive" | "shared" => Right(())
        case x => Left("Invalid mode, use:[shared, exclusive]")
      })
      .text("Worker mode: 'exclusive' or 'shared'")

    opt[Int]("max-jobs").optional().action((x, a) => a.copy(maxJobs = x))

    opt[Duration]("downtime").optional().action((x, a) => a.copy(downtime = x))

    opt[Duration]("spark-streaming-duration").optional().action((x, a) => a.copy(streamingDuration = x))

    opt[String]("log-service").optional().action((x, a) => a.copy(logServiceAddress = Some(x)))
  }

  def parse(args: Seq[String]): WorkerArguments = {
    val localAddress = NetUtils.findLocalInetAddress().getHostAddress.toString
    parser.parse(args, WorkerArguments(bindAddress = s"$localAddress:0")) match {
      case Some(workerArgs) => workerArgs
      case None => sys.exit(1)
    }
  }

}

object Worker extends App with Logger {

  import scala.collection.JavaConverters._

  try {

    val arguments = WorkerArguments.parse(args)
    val name = arguments.name

    val mode = arguments.workerMode
    logger.info(s"Try starting on spark: ${org.apache.spark.SPARK_VERSION}")

    val context = try { arguments.createNamedContext }
    catch {
      case e: Throwable =>
        throw new RuntimeException("Spark context initialization failed", e)
    }

    val seedNodes = Seq(arguments.masterNode).asJava
    val roles = Seq(s"worker-$name").asJava
    val config = ConfigFactory.load("worker")
      .withValue("akka.cluster.seed-nodes", ConfigValueFactory.fromIterable(seedNodes))
      .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(roles))
      .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(arguments.bindHost))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(arguments.bindPort))

    val system = ActorSystem("mist", config)

    val props = ClusterWorker.props(
      name = arguments.name,
      workerProps = WorkerActor.props(mode, context)
    )
    system.actorOf(props, s"worker-$name")

    val msg =
      s"""Worker $name is started
          |settings:
          |  mode = $mode
          |  sparkConf = ${arguments.sparkConfString}
    """.stripMargin

    logger.info(msg)

    system.awaitTermination()
    logger.info(s"Shutdown worker application $name ${arguments.contextName}")
    context.stop()

  } catch {
    case e: Throwable =>
    logger.error("Fatal error", e)
  }

}

