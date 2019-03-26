package io.hydrosphere.mist.master.store

import java.util.concurrent.{ExecutorService, Executors, Future, TimeUnit}

import cats.arrow.FunctionK
import cats.effect._
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext


/**
  * Unmanaged implementation of the HikariDataSource.
  *
  * This implementation is not uses cat.effect.Resource for cleaning resources.
  * Client must close an instance of the HikariDataSourceTransactor by himself.
  *
  * @param config Already configured HikariConfig
  * @param poolSize Connections pool size
  * @param awaitShutdown How much time await shutdown each thread pool
  *        (connection and transaction execution contexts).
  * @see #shutdown
  * @author Andrew Saushkin
  */
class HikariDataSourceTransactor(config: HikariConfig, poolSize: Int = 32, awaitShutdown: Long = 1000) extends Logger {

  protected val ce: ExecutorService = Executors.newFixedThreadPool(poolSize) // our connect EC
  protected val te: ExecutorService = Executors.newCachedThreadPool    // our transaction EC

  val ds = new HikariDataSource(config)

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val transactor: Aux[IO, HikariDataSource] =
    Transactor.fromDataSource[IO](ds, ExecutionContext.fromExecutor(ce), ExecutionContext.fromExecutor(te))
  
  private def shutdownExecutorService(awaitShutdown: Long, es: ExecutorService, debugInfo: String = ""): Unit = {
    logger.info(s"Shutting down executor service $debugInfo")
    if (es.isTerminated) {
      logger.warn(s"ExecutorService $es ($debugInfo) had not been initialized before shutdown. Operation rejected.")
    } else {
      es.shutdown()
      if (!es.awaitTermination(awaitShutdown, TimeUnit.MILLISECONDS)) {
        logger.warn(s"ExecutorService: $es ($debugInfo) has not been shutdown properly in $awaitShutdown ms. Force shutdown.")
        es.shutdownNow()
      }
    }
    logger.info(s"Executor service $debugInfo shutdown complete")
  }

  /**
    * Client *must* call this method after using HikariDataSourceTransactor
    */
  def shutdown(): Unit = {
    if (!ds.isClosed) {
      logger.info("Closing Hikari data source")
      ds.close()
    } else {
      logger.warn("Hikari datasource had not been properly initialized before closing")
    }

    shutdownExecutorService(awaitShutdown, ce, "connections EC")
    shutdownExecutorService(awaitShutdown, te, "tx EC")
  }
}
