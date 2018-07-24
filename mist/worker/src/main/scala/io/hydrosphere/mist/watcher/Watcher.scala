package io.hydrosphere.mist.watcher

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.utils.akka.WhenTerminated
import io.hydrosphere.mist.utils.{Logger, NetUtils}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

case class WatcherArgs(
  bindAddress: String = "localhost:0",
  masterAddress: String = ""
)

object WatcherArgs {

  val parser = new scopt.OptionParser[WatcherArgs]("mist-worker") {

    override def errorOnUnknownArgument: Boolean = false
    override def reportWarning(msg: String): Unit = {}

    head("mist-watcher")

    opt[String]("bind-address").action((x, a) => a.copy(bindAddress = x))

    opt[String]("master").action((x, a) => a.copy(masterAddress = x))
      .text("host:port to master")
  }

  def parse(args: Seq[String]): Option[WatcherArgs] = {
    val localAddress = NetUtils.findLocalInetAddress().getHostAddress
    parser.parse(args, WatcherArgs(bindAddress = s"$localAddress:0"))
  }

  def forceParse(args: Seq[String]): WatcherArgs = parse(args) match {
    case Some(workerArgs) => workerArgs
    case None => sys.exit(1)
  }

}

object Watcher extends App with Logger {

  try {
    val arguments = WatcherArgs.forceParse(args)

    val config = ConfigFactory.load("job-extractor")
    implicit val system = ActorSystem("mist-watcher", config)
    implicit val ec: ExecutionContext = system.dispatcher

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

    def remotePath(name: String): String = {
      s"akka.tcp://mist@${arguments.masterAddress}/user/$name"
    }

    val heathRef = resolveRemote(remotePath(CommonData.HealthActorName))
    WhenTerminated(heathRef, {
      logger.info("Remote system was terminated, shutdown app")

      system.terminate()
    })

  } catch {
    case e: Throwable =>
      logger.error("Fatal error", e)
      sys.exit(1)
  } finally {
    sys.exit()
  }

}


