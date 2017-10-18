package io.hydrosphere.mist.job

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.core.CommonData.RegisterJobInfoProvider
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.runners.ArtifactDownloader

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class JobInfoProviderArguments(
  masterHost: String = "localhost",
  httpPort: Int = 2004,
  clusterPort: Int = 2551,
  savePath: String = "/tmp"
) {

  def clusterAddr: String = s"$masterHost:$clusterPort"
}

object JobInfoProviderArguments {
  val parser = new scopt.OptionParser[JobInfoProviderArguments]("mist-job-executor") {

    override def errorOnUnknownArgument: Boolean = false

    override def reportWarning(msg: String): Unit = {}

    head("mist-job-executor")

    opt[String]("master").action((x, a) => a.copy(masterHost = x))
      .text("host to master")
    opt[Int]("http-port").action((x, a) => a.copy(httpPort = x))
      .text("http port of master")
    opt[Int]("cluster-port").action((x, a) => a.copy(clusterPort = x))
      .text("cluster port of master")
    opt[String]("save-path").action((x, a) => a.copy(savePath = x))
      .text("storage path where jobs will be downloaded")
  }

  def parse(args: Seq[String]): Option[JobInfoProviderArguments] =
    parser.parse(args, JobInfoProviderArguments())

}


object JobInfoProvider extends App with Logger {

  try {
    val jobExtractorArguments = JobInfoProviderArguments.parse(args) match {
      case Some(x) => x
      case None =>
        logger.error("Please provide master address through --master option")
        throw new IllegalStateException("please provide arguments")
    }
    val config = ConfigFactory.load("job-extractor")
    val system = ActorSystem("mist", config)
    implicit val ec: ExecutionContext = system.dispatcher

    val artifactDownloader = ArtifactDownloader.create(
      jobExtractorArguments.masterHost,
      jobExtractorArguments.httpPort,
      jobExtractorArguments.savePath
    )

    val jobInfoProviderRef = system.actorOf(JobInfoProviderActor.props(artifactDownloader), "job-extractor")

    val jobInfoProviderRegistererName =
      s"akka.tcp://mist@${jobExtractorArguments.clusterAddr}/user/${CommonData.JobExecutorRegisterActorName}"
    system.actorSelection(jobInfoProviderRegistererName)
      .resolveOne(10 second)
      .onComplete {
        case Success(ref) =>
          ref ! RegisterJobInfoProvider(jobInfoProviderRef)
        case Failure(ex) =>
          logger.error(ex.getMessage, ex)
          sys.exit(-1)
      }

    system.awaitTermination()

    sys.addShutdownHook {
      logger.info("Shutdown job extractor")
      system.shutdown()
    }
  } catch {
    case e: Exception =>
      logger.error(e.getMessage, e)
  }


}