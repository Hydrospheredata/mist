package io.hydrosphere.mist.master.store

import com.zaxxer.hikari.HikariConfig
import io.hydrosphere.mist.master.{DbConfig, JobDetails, JobDetailsRequest, JobDetailsResponse}

import scala.concurrent.{ExecutionContext, Future}

trait JobRepository {

  def remove(jobId: String): Future[Unit]

  def get(jobId: String): Future[Option[JobDetails]]

  def update(jobDetails: JobDetails): Future[Unit]

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]

  def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getAll(limit: Int, offset: Int): Future[Seq[JobDetails]] = getAll(limit, offset, Seq.empty)

  def clear(): Future[Unit]

  def running(): Future[Seq[JobDetails]] = filteredByStatuses(JobDetails.Status.inProgress)

  def path(jobId: String)(f: JobDetails => JobDetails)(implicit ec: ExecutionContext): Future[Unit] = {
    get(jobId).flatMap {
      case Some(d) => update(f(d))
      case None => Future.failed(new IllegalStateException(s"Not found job: $jobId"))
    }
  }

  def getJobs(req: JobDetailsRequest): Future[JobDetailsResponse]
}

object JobRepository {
  def apply(config: DbConfig): JobRepository = {

    if (!config.filePath.isEmpty) {
      H2JobsRepository(config.filePath.get)
    } else {
      if (!config.jdbcUrl.isEmpty) {
        val url = config.jdbcUrl.get
        if (url.startsWith("jdbc:h2") || url.startsWith("jdbc:postgresql")) {
          val hikariConfig = new HikariConfig()
          hikariConfig.setDriverClassName(config.driverClass.get)
          hikariConfig.setJdbcUrl(config.jdbcUrl.get)
          hikariConfig.setUsername(config.username.get)
          hikariConfig.setPassword(config.password.get)

          val hikari = new HikariDataSourceTransactor(hikariConfig, config.poolSize.get)
          new HikariJobRepository(hikari)
        } else {
          throw new RuntimeException(s"Only H2 and PostgreSQL databases supported now")
        }
      } else {
        throw new RuntimeException(s"You must setup either db.filepath either db.jdbcUrl parameter")
      }
    }
  }
}
