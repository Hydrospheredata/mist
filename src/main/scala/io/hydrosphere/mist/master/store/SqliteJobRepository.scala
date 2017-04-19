package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.{JobParameters, JobResponseOrError}
import io.hydrosphere.mist.utils.json.JobDetailsJsonSerialization
import org.flywaydb.core.Flyway
import spray.json.{pimpAny, pimpString}
import slick.driver.SQLiteDriver.api._
import slick.lifted.ProvenShape

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import scala.concurrent.Await


class SqliteJobRepository(filePath: String) extends JobRepository with JobDetailsJsonSerialization with Logger {

  private val db = {
    val jdbcPath = s"jdbc:sqlite:$filePath"
    val flyway = new Flyway ()
    flyway.setLocations("/db/migrations")
    flyway.setDataSource(jdbcPath, null, null)
    flyway.migrate()
    Database.forURL(jdbcPath, driver="org.sqlite.JDBC")
  }

  private implicit def string2Source = MappedColumnType.base[JobDetails.Source, String](
    source => source.toString,
    string => JobDetails.Source(string)
  )

  private implicit def string2Status = MappedColumnType.base[JobDetails.Status, String](
    status => status.toString,
    string => JobDetails.Status(string)
  )

  private implicit def string2JobResponseOrError = MappedColumnType.base[JobResponseOrError, String](
    jobResponseOrError => jobResponseOrError.toJson.compactPrint,
    string => string.parseJson.convertTo[JobResponseOrError]
  )

  private implicit def string2JobParameters = MappedColumnType.base[JobParameters, String](
    jobParameters => jobParameters.toJson.compactPrint,
    string => string.parseJson.convertTo[JobParameters]
  )

  private implicit def string2Action = MappedColumnType.base[Action, String](
    action => action.toString,
    string => Action(string)
  )

  private implicit def tuple2JobConfiguration(tuple: (String, String, String, JobParameters, Option[String], Option[String], Action)): JobExecutionParams = tuple match {
    case (path, className, namespace, parameters, externalId, route, action) =>
      JobExecutionParams(path, className, namespace, parameters, externalId, route, action)
  }


  private class JobDetailsTable(tag: Tag) extends Table[JobDetails](tag, "job_details") {
    
    def path: Rep[String] = column[String]("path")
    def className: Rep[String] = column[String]("class_name")
    def namespace: Rep[String] = column[String]("namespace")
    def parameters: Rep[JobParameters] = column[JobParameters]("parameters")
    def externalId: Rep[Option[String]] = column[Option[String]]("external_id")
    def route: Rep[Option[String]] = column[Option[String]]("route")
    def action: Rep[Action] = column[Action]("action")
    def source: Rep[JobDetails.Source] = column[JobDetails.Source]("source")
    def jobId: Rep[String] = column[String]("job_id", O.PrimaryKey)
    def startTime: Rep[Option[Long]] = column[Option[Long]]("start_time")
    def endTime: Rep[Option[Long]] = column[Option[Long]]("end_time")
    def jobResult: Rep[Option[JobResponseOrError]] = column[Option[JobResponseOrError]]("job_result")
    def status: Rep[JobDetails.Status] = column[JobDetails.Status]("status")
    
    override def * : ProvenShape[JobDetails] = {
      val shapedValue = (
        path,
        className,
        namespace,
        parameters,
        externalId,
        route,
        action,
        source,
        jobId,
        startTime,
        endTime,
        jobResult,
        status
      ).shaped
      shapedValue.<>({
        tuple => JobDetails.apply(
          configuration = JobExecutionParams(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6, tuple._7),
          source = tuple._8,
          jobId = tuple._9,
          startTime = tuple._10,
          endTime = tuple._11,
          jobResult = tuple._12,
          status = tuple._13  
        )
      }, {
        (j: JobDetails) => Some {
          (j.configuration.path, j.configuration.className, j.configuration.namespace, j.configuration.parameters,
           j.configuration.externalId, j.configuration.route, j.configuration.action, j.source, j.jobId, j.startTime,
           j.endTime, j.jobResult, j.status)
        }
      })
    }
  }
  
  private val jobs = TableQuery[JobDetailsTable]
  
  private def run[A](query: DBIOAction[A, NoStream, Nothing]): A = Await.result(db.run(query), atMost = 10 seconds)
  
  override def remove(jobId: String): Unit = {
    run(jobs.filter(_.jobId === jobId).delete)
  }

  override def get(jobId: String): Option[JobDetails] = {
    run(jobs.filter(_.jobId === jobId).result).headOption
  }

  override def getByExternalId(id: String): Option[JobDetails] = {
    run(jobs.filter(_.externalId === id).result).headOption
  }

  override def size: Long = {
    run(jobs.length.result).toLong
  }

  override def clear(): Unit = {
    run(jobs.delete)
  }

  override def update(jobDetails: JobDetails): Unit = {
    run(jobs.insertOrUpdate(jobDetails))
  }

  override def filteredByStatuses(statuses: List[Status]): List[JobDetails] = {
    run(jobs.filter(_.status inSetBind statuses).result).toList
  }

  override def queuedInNamespace(namespace: String): List[JobDetails] = {
    run(jobs.filter(j => j.status.asColumnOf[String] === JobDetails.Status.Queued.toString && j.namespace === namespace).result).toList
  }

  override def runningInNamespace(namespace: String): List[JobDetails] = {
    run(jobs.filter(j => j.status.asColumnOf[String] === JobDetails.Status.Running.toString && j.namespace === namespace).result).toList
  }
}

object SqliteJobRepository extends SqliteJobRepository(MistConfig.History.filePath)
