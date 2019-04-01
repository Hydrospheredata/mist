package io.hydrosphere.mist.master.store

import cats.syntax.either._
import java.nio.file.Paths

import io.hydrosphere.mist.master.DbConfig

case class JobRepoSetup(
  driverClass: String,
  jdbcUrl: String,
  username: Option[String],
  password: Option[String],
  poolSize: Int,
  migrationPath: Option[String],
  jobRequestSql: JobRequestSql
)

object JobRepoSetup {
  
  val H2MigrationPath: String = "/db/migrations/h2"
  val PgMigrationPath: String = "/db/migrations/postgresql"
  
  def apply(config: DbConfig): Either[Throwable, JobRepoSetup] = config match {
    case DbConfig.H2OldConfig(path) =>
      val absolute = Paths.get(path).toAbsolutePath
      val out= JobRepoSetup(
        "org.h2.Driver",
        s"jdbc:h2:file:$absolute;DATABASE_TO_UPPER=false",
        None, None, 10,
        migrationPath = Some(H2MigrationPath),
        new H2JobRequestSql
      )
      out.asRight
      
    case h2: DbConfig.JDBCDbConfig if dbIs(h2, "h2") =>
      fromJdbcConf(h2, H2MigrationPath, new H2JobRequestSql).asRight
      
    case pg: DbConfig.JDBCDbConfig if dbIs(pg, "postgresql") =>
      fromJdbcConf(pg, PgMigrationPath, new PgJobRequestSql).asRight
      
    case _: DbConfig.JDBCDbConfig =>
      new RuntimeException(s"Only H2 and PostgreSQL databases are supported now").asLeft
  }
  
  private def fromJdbcConf(conf: DbConfig.JDBCDbConfig, migrationPath: String, reqSql: JobRequestSql): JobRepoSetup = {
    JobRepoSetup(
      conf.driverClass,
      conf.jdbcUrl,
      conf.username,
      conf.password,
      conf.poolSize,
      if (conf.migration) Some(migrationPath) else None,
      reqSql
    )
  }
  
  private def dbIs(conf: DbConfig.JDBCDbConfig, name: String): Boolean = conf.jdbcUrl.startsWith(s"jdbc:$name")
  
}
