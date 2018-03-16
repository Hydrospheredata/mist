package io.hydrosphere.mist.master

import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  try {
    logger.info("Starting mist...")
    val appArguments = MasterAppArguments.parse(args) match {
      case Some(arg) => arg
      case None => sys.exit(1)
    }
    val config: MasterConfig = MasterConfig.load(appArguments.configPath)
    val starting = MasterServer.start(config, appArguments.routerConfigPath)
    val master = Await.result(starting, Duration.Inf)
    logger.info("Mist master started")


    sys addShutdownHook {
      logger.info("Received shutdown - start application termination")
      Await.result(master.stop(), Duration.Inf)
      logger.info("Mist master stopped")
    }
  } catch {
    case e: Throwable =>
      logger.error(s"Unexpected error: ${e.getMessage}", e)
      sys.exit(1)
  }


}
