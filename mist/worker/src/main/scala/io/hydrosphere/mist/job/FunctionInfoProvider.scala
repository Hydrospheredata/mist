package io.hydrosphere.mist.job

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.core.CommonData.RegisterJobInfoProvider
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.akka.WhenTerminated

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class FunctionInfoProviderArguments(
  masterHost: String = "localhost",
  clusterPort: Int = 2551,
  savePath: String = "/tmp",
  cacheEntryTtl: FiniteDuration = 3600 seconds
) {

  def clusterAddr: String = s"$masterHost:$clusterPort"
}

object FunctionInfoProviderArguments {
  val parser = new scopt.OptionParser[FunctionInfoProviderArguments]("mist-job-executor") {

    override def errorOnUnknownArgument: Boolean = false

    override def reportWarning(msg: String): Unit = {}

    head("mist-job-executor")

    opt[String]("master").action((x, a) => a.copy(masterHost = x))
      .text("host to master")
    opt[Int]("cluster-port").action((x, a) => a.copy(clusterPort = x))
      .text("cluster port of master")
    opt[String]("save-path").action((x, a) => a.copy(savePath = x))
      .text("storage path where jobs will be downloaded")
    opt[Long]("cache-entry-ttl").action((x, a) => a.copy(cacheEntryTtl = x milliseconds))
      .text("Cache entry ttl value in milliseconds")
  }

  def parse(args: Seq[String]): Option[FunctionInfoProviderArguments] =
    parser.parse(args, FunctionInfoProviderArguments())

}


object FunctionInfoProvider extends App with Logger {

  try {
    val functionInfoProviderArguments = FunctionInfoProviderArguments.parse(args) match {
      case Some(x) => x
      case None =>
        throw new IllegalStateException("please provide arguments")
    }
    val config = ConfigFactory.load("job-extractor")
    implicit val system = ActorSystem("mist-info-provider", config)
    implicit val ec: ExecutionContext = system.dispatcher

    val functionInfoProviderRef = system.actorOf(
      FunctionInfoProviderActor.props(
        FunctionInfoExtractor(),
        functionInfoProviderArguments.cacheEntryTtl
      ),"job-info-provider")

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
      s"akka.tcp://mist@${functionInfoProviderArguments.clusterAddr}/user/$name"
    }

    val registerRef = resolveRemote(remotePath(CommonData.FunctionInfoProviderRegisterActorName))
    val heathRef = resolveRemote(remotePath(CommonData.HealthActorName))

    WhenTerminated(heathRef, {
      logger.info("Remote system was terminated, shutdown app")
      functionInfoProviderRef ! PoisonPill
      system.terminate()
    })

    registerRef ! RegisterJobInfoProvider(functionInfoProviderRef)


    Await.result(system.whenTerminated, Duration.Inf)

    sys.exit()
  } catch {
    case e: Exception =>
      logger.error(e.getMessage, e)
  }



}