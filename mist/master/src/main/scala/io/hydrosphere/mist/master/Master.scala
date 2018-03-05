package io.hydrosphere.mist.master

import cats._
import cats.implicits._
import cats.data.Reader
import cats.syntax._
import com.typesafe.config.ConfigValueFactory
import io.hydrosphere.mist.utils.{Logger, NetUtils}

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
    val config: MasterConfig = configuration(appArguments.configPath)

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

  def configuration(path: String): MasterConfig = {
    val config = MasterConfig.load(path)
    val autoHost = Eval.later(NetUtils.findLocalInetAddress().getHostAddress)

    def updClusterHost(cfg: MasterConfig): MasterConfig = {
      import cfg._
      logger.info(s"Automatically update cluster host $cfg to ${autoHost.value}")
      val upd = cluster.copy(host = autoHost.value)
      cfg.copy(cluster = upd)
        .copy(raw = raw.withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(autoHost.value)))
    }

    def updHttpHost(cfg: MasterConfig): MasterConfig = {
      import cfg._
      logger.info(s"Automatically update http host $cfg to ${autoHost.value}")
      val upd = http.copy(host = autoHost.value)
      cfg.copy(http = upd)
    }

    def updLogHost(cfg: MasterConfig): MasterConfig = {
      import cfg._
      logger.info(s"Automatically update log host $cfg to ${autoHost.value}")
      val upd = logs.copy(host = autoHost.value)
      cfg.copy(logs = upd)
    }

    def needPatchHost(host: String): Boolean = host.isEmpty || host == "0.0.0.0"

    val x1 = (cfg: MasterConfig) => {
      if (needPatchHost(cfg.cluster.host)) updClusterHost(cfg) else cfg
    }
    val x2 = (cfg: MasterConfig) => {
      if (needPatchHost(cfg.http.host)) updHttpHost(cfg) else cfg
    }
    val x3 = (cfg: MasterConfig) => {
      if (needPatchHost(cfg.logs.host)) updLogHost(cfg) else cfg
    }
    val x = x1 >>> x2 >>> x3
    x(config)
  }

}
