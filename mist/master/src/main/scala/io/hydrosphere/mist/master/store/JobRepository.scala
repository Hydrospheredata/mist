package io.hydrosphere.mist.master.store

import cats.implicits._
import com.zaxxer.hikari.HikariConfig
import io.hydrosphere.mist.master.{DbConfig, JobDetails, JobDetailsRequest, JobDetailsResponse}
import javax.sql.DataSource
import org.flywaydb.core.Flyway

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
  
  def create(config: DbConfig): Either[Throwable, HikariJobRepository] = {
    for {
      setup  <- JobRepoSetup(config)
      trns   <- transactor(setup)
    } yield new HikariJobRepository(trns, setup.jobRequestSql)
  }
  
  private def transactor(setup: JobRepoSetup): Either[Throwable, HikariDataSourceTransactor] = {
    Either.catchNonFatal {
      val transactor = new HikariDataSourceTransactor(hikariConfig(setup), setup.poolSize)
      setup.migrationPath match {
        case None => transactor
        case Some(path) =>
          migrate(path, transactor.ds)
          transactor
      }
    }
  }
  
  private def migrate(migrationPath: String, ds: DataSource): Unit = {
    val flyway = new Flyway()
    flyway.setBaselineOnMigrate(true)
    flyway.setLocations(migrationPath)
    flyway.setDataSource(ds)
    flyway.migrate()
  }
  
  private def hikariConfig(setup: JobRepoSetup): HikariConfig = {
    import setup._
    
    val configure =
      SetterFunc[HikariConfig](driverClass)(_.setDriverClassName(_)) >>>
      SetterFunc[HikariConfig](jdbcUrl)(_.setJdbcUrl(_)) >>>
      SetterFunc[HikariConfig].opt(username)(_.setUsername(_)) >>>
      SetterFunc[HikariConfig].opt(password)(_.setPassword(_))
    
    configure(new HikariConfig())
  }
  
  object SetterFunc {
    
    def void[A, B](b: B)(f: (A, B) => Unit): A => A =
      (a: A) => { f(a, b); a}
    
    def apply[A]: Partial[A] = new Partial[A]
    
    class Partial[A] { self =>
      
      def apply[B](b: B)(f: (A, B) => Unit): A => A =
        (a: A) => { f(a, b); a }
      
      def opt[B](b: Option[B])(f: (A, B) => Unit): A => A =
        b match {
          case Some(v) => self(v)(f)
          case None => identity[A]
        }
    }
    
    def opt[A, B](b: Option[B])(f: (A, B) => Unit): A => A =
      b match {
        case Some(v) => void(v)(f)
        case None => identity[A]
      }
  }
  
}
