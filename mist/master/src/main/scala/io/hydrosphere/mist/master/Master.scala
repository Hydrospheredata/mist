package io.hydrosphere.mist.master

import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.reflectiveCalls

/** This object is entry point of Mist project */
object Master extends App with Logger {

  val appArguments = MasterAppArguments.parse(args) match {
    case Some(arg) => arg
    case None => sys.exit(1)
  }
  val config: MasterConfig = MasterConfig.load(appArguments.configPath)
  val master = MasterServer(config, appArguments.routerConfigPath)

  master.start()

  sys addShutdownHook {
    Await.result(master.stop(), Duration.Inf)
  }

}
