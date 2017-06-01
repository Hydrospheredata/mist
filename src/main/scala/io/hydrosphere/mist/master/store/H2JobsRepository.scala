package io.hydrosphere.mist.master.store

import java.nio.file.Paths

import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.jobs.{JobDetails, _}
import io.hydrosphere.mist.master.interfaces.http.JsonCodecs._
import io.hydrosphere.mist.utils.TypeAlias.JobParameters
import slick.driver.H2Driver.api._
import slick.lifted.ProvenShape
import spray.json.{pimpAny, pimpString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait JobsTable {

  implicit def string2Source = MappedColumnType.base[JobDetails.Source, String](
    source => source.toString,
    string => JobDetails.Source(string)
  )

  implicit def string2Status = MappedColumnType.base[JobDetails.Status, String](
    status => status.toString,
    string => JobDetails.Status(string)
  )

  implicit def string2JobResponseOrError = MappedColumnType.base[Either[String, Map[String, Any]], String](
    jobResponseOrError => jobResponseOrError.toJson.compactPrint,
    string => string.parseJson.convertTo[Either[String, Map[String, Any]]]
  )

  implicit def string2JobParameters = MappedColumnType.base[JobParameters, String](
    jobParameters => jobParameters.toJson.compactPrint,
    string => string.parseJson.convertTo[JobParameters]
  )

  implicit def string2Action = MappedColumnType.base[Action, String](
    action => action.toString,
    string => Action(string)
  )

//  implicit def tuple2JobConfiguration(tuple: (String, String, String, JobParameters, Option[String], Option[String], Action)): JobExecutionParams = tuple match {
//    case (path, className, namespace, parameters, externalId, route, action) =>
//      JobExecutionParams(path, className, namespace, parameters, externalId, route, action)
//  }

  class JobDetailsTable(tag: Tag) extends Table[JobDetails](tag, "job_details") {

    def path = column[String]("path")
    def className = column[String]("class_name")
    def namespace = column[String]("namespace")
    def parameters = column[JobParameters]("parameters")
    def externalId = column[Option[String]]("external_id")
    def endpoint = column[String]("endpoint")
    def action = column[Action]("action")
    def source = column[JobDetails.Source]("source")
    def jobId = column[String]("job_id", O.PrimaryKey)
    def startTime = column[Option[Long]]("start_time")
    def endTime = column[Option[Long]]("end_time")
    def jobResult = column[Option[Either[String, Map[String, Any]]]]("job_result")
    def status = column[JobDetails.Status]("status")

    override def * : ProvenShape[JobDetails] = {
      val shapedValue = (
        path,
        className,
        namespace,
        parameters,
        externalId,
        endpoint,
        action,
        source,
        jobId,
        startTime,
        endTime,
        jobResult,
        status
        ).shaped
      shapedValue.<>({
        tuple => JobDetails(
          endpoint = tuple._6,
          jobId = tuple._9,
          params = JobParams(tuple._1, tuple._2, tuple._4, tuple._7),
          context = tuple._3,
          externalId = tuple._5,
          source = tuple._8,
          startTime = tuple._10,
          endTime = tuple._11,
          jobResult = tuple._12,
          status = tuple._13
        )
      }, {
        (j: JobDetails) => Some(
          (j.params.filePath, j.params.className, j.context, j.params.arguments, j.externalId,
            j.endpoint, j.params.action, j.source, j.jobId, j.startTime, j.endTime, j.jobResult, j.status)
        )
      })
    }
  }

  val table = TableQuery[JobDetailsTable]
}

class H2JobsRepository(db: Database) extends JobRepository with JobsTable {

  private def run[A](query: DBIOAction[A, NoStream, Nothing]): Future[A]= db.run(query)

  override def remove(id: String): Future[Unit] = {
    val query = table.filter(_.jobId === id).delete
    run(query).map(_ => ())
  }

  override def get(id: String): Future[Option[JobDetails]] = {
    run(table.filter(_.jobId === id).result).map(_.headOption)
  }

  override def getByExternalId(id: String): Future[Seq[JobDetails]] = {
    run(table.filter(_.externalId === id).result)
  }

  override def update(jobDetails: JobDetails): Future[Unit] = {
    run(table.insertOrUpdate(jobDetails)).map(_ => ())
  }

  override def filteredByStatuses(statuses: Seq[Status]): Future[Seq[JobDetails]] = {
    run(table.filter(_.status inSetBind statuses).result)
  }

  override def all(): Future[Seq[JobDetails]] =
    run(table.result)

  override def clear(): Future[Unit] = run(table.delete).map(_ => ())

  override def getByEndpointId(id: String): Future[Seq[JobDetails]] = {
    run(table.filter(_.endpoint === id).result)
  }
}

object H2JobsRepository {

  import org.flywaydb.core.Flyway

  def apply(filePath: String): H2JobsRepository = {
    val absolute = Paths.get(filePath).toAbsolutePath
    val url = s"jdbc:h2:file:$absolute;DATABASE_TO_UPPER=false"
    migrateDb(url)

    val db = Database.forURL(url)
    new H2JobsRepository(db)
  }

  private def migrateDb(url: String): Unit = {
    val flyway = new Flyway()
    flyway.setLocations("/db/migrations")
    flyway.setDataSource(url, null, null)
    flyway.migrate()

  }
}
