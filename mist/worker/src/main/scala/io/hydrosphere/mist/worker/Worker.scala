package io.hydrosphere.mist.worker

import java.nio.file.{Path, Paths}

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.hydrosphere.mist.utils.{Logger, NetUtils}
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._

class WorkerDirectory(root: Path, id: String) extends Logger {
  val workerDir = root.resolve(id)

  def mkDir: Path = {
    if (workerDir.toFile.exists()) {
      logger.warn(s"Directory in path $workerDir already exists. It may cause errors in worker lifecycle!")
      workerDir
    } else {
      FileUtils.forceMkdir(workerDir.toFile)
      workerDir
    }
  }

  def cleanUp: Unit = {
    FileUtils.deleteQuietly(workerDir.toFile)
  }
}

case class WorkerArguments(
  bindAddress: String = "localhost:0",
  masterAddress: String = "",
  name: String = ""
) {

  def masterNode: String = s"akka.tcp://mist@$masterAddress"

  def bindHost: String = bindAddress.split(":")(0)
  def bindPort: Int = bindAddress.split(":")(1).toInt

}


object WorkerArguments {

  val parser = new scopt.OptionParser[WorkerArguments]("mist-worker") {

    override def errorOnUnknownArgument: Boolean = false
    override def reportWarning(msg: String): Unit = {}

    head("mist-worker")

    opt[String]("bind-address").action((x, a) => a.copy(bindAddress = x))

    opt[String]("master").action((x, a) => a.copy(masterAddress = x))
      .text("host:port to master")

    opt[String]("name").action((x, a) => a.copy(name = x))
      .text("Uniq name of worker")

  }

  def parse(args: Seq[String]): Option[WorkerArguments] = {
    val localAddress = NetUtils.findLocalInetAddress().getHostAddress
    parser.parse(args, WorkerArguments(bindAddress = s"$localAddress:0"))
  }

  def forceParse(args: Seq[String]): WorkerArguments = parse(args) match {
    case Some(workerArgs) => workerArgs
    case None => sys.exit(1)
  }

}

object Worker extends App with Logger {
  val BaseDir = "/tmp"
  try {

    val arguments = WorkerArguments.forceParse(args)
    val name = arguments.name

    logger.info(s"Try starting on spark: ${org.apache.spark.SPARK_VERSION}, master: ${arguments.masterNode}")

    val config = ConfigFactory.load("worker")
      .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(arguments.bindHost))
      .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(arguments.bindPort))

    val system = ActorSystem(s"mist-worker-$name", config)

    def resolveRemote(path: String): ActorRef = {
      val ref = system.actorSelection(path).resolveOne(10 seconds)
      try {
        Await.result(ref, Duration.Inf)
      } catch {
        case e: Throwable =>
          logger.error(s"Couldn't resolve remote path $path", e)
          sys.exit(-1)
      }
    }

    val regHub = resolveRemote(arguments.masterNode + "/user/regHub")
    val workDir = Paths.get(BaseDir, s"worker-${arguments.name}")
    val workDirFile = workDir.toFile
    if (workDirFile.exists()) {
      logger.warn(s"Directory in path $workDir already exists. It may cause errors in worker lifecycle!")
    } else {
      FileUtils.forceMkdir(workDirFile)
    }
    val props = MasterBridge.props(arguments.name, workDir, regHub)
    system.actorOf(props, s"worker-$name")

    val msg = s"Worker $name is started, context ${arguments.name}"
    logger.info(msg)

    Await.result(system.whenTerminated, Duration.Inf)
    logger.info(s"Shutdown worker application $name ${arguments.name}")
    FileUtils.deleteQuietly(workDirFile)
    sys.exit()
  } catch {
    case e: Throwable =>
      logger.error("Fatal error", e)
      sys.exit(1)
  }

}

