package io.hydrosphere.mist.master.store

import cats._
import cats.implicits._
import cats.syntax._

import java.nio.file.Paths

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
  
  def apply(config: DbConfig): Either[Throwable, JobRepository] = {
    hikariConfig(config).map(cfg => {
      val poolSize = config match {
        case jdbc:DbConfig.JDBCDbConfig => jdbc.poolSize
        case _:DbConfig.H2OldConfig => 10
      }
      val hikari = new HikariDataSourceTransactor(cfg, poolSize)
      new HikariJobRepository(hikari)
    })
  }
  
  private def hikariConfig(config: DbConfig): Either[Throwable, HikariConfig] = {
    val (driver, url, user, passwd) = config match {
      case DbConfig.H2OldConfig(path) =>
        val absolute = Paths.get(path).toAbsolutePath
        ("org.h2.Driver", s"jdbc:h2:file:$absolute;DATABASE_TO_UPPER=false", None, None)
      case DbConfig.JDBCDbConfig(_, driver, url, user, passwd) =>
        (driver, url, user, passwd)
    }
    
    if (isSupported(url)) {
      val configure =
        SetterFunc[HikariConfig](driver)(_.setDriverClassName(_)) >>>
        SetterFunc[HikariConfig](url)(_.setJdbcUrl(_)) >>>
        SetterFunc[HikariConfig].opt(user)(_.setUsername(_)) >>>
        SetterFunc[HikariConfig].opt(passwd)(_.setPassword(_))
      
      configure(new HikariConfig()).asRight
    } else {
      new RuntimeException(s"Only H2 and PostgreSQL databases supported now").asLeft
    }
  }
  
  private def isSupported(jdbcUrl: String): Boolean = {
    def dbIs(url: String, name: String): Boolean = url.startsWith(s"jdbc:$name")
    
    dbIs(jdbcUrl, "h2") || dbIs(jdbcUrl, "postgresql")
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
