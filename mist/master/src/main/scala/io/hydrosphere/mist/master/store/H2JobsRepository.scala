package io.hydrosphere.mist.master.store

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.{FilterClause, JobDetails, JobDetailsRequest, JobDetailsResponse}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import JsonCodecs._
import mist.api.data.{JsData, JsMap}
import slick.driver.H2Driver.api._
import slick.lifted.ProvenShape
import spray.json.{JsObject, JsString, enrichAny, enrichString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

trait JobsTable {

  implicit def string2Source = MappedColumnType.base[JobDetails.Source, String](
    source => source.toString,
    string => JobDetails.Source(string)
  )

  implicit def string2Status = MappedColumnType.base[JobDetails.Status, String](
    status => status.toString,
    string => JobDetails.Status(string)
  )

  implicit def string2JobResponseOrError = MappedColumnType.base[Either[String, JsData], String](
    jobResponseOrError => {
      val jsValue = jobResponseOrError match {
        case Left(err) => JsObject("error" -> JsString(err))
        case Right(data) => JsObject("result" -> data.toJson)
      }
      jsValue.compactPrint
    },
    string => {
      string.parseJson match {
        case obj @ JsObject(fields) =>
          val maybeErr = fields.get("error").flatMap({
            case JsString(err) => Some(err)
            case x => None
          })
          maybeErr match {
            case None => Right(fields.get("result").get.convertTo[JsData])
            case Some(err) => Left(err)
          }
        // TODO: backward compatibility
        case JsString(err) => Left(err)
        case _ => throw new IllegalArgumentException(s"can not deserialize $string to Job response")
      }
    }
  )

  implicit def string2JobParameters = MappedColumnType.base[JsMap, String](
    jobParameters => jobParameters.toJson.compactPrint,
    string => string.parseJson.convertTo[JsMap]
  )

  implicit def string2Action = MappedColumnType.base[Action, String](
    action => action.toString,
    //TODO: now train is the same as execute
    string => string match {
      case "serve" => Action.Serve
      case "train" => Action.Execute
      case _ => Action.Execute
    }
  )

  class JobDetailsTable(tag: Tag) extends Table[JobDetails](tag, "job_details") {

    def path = column[String]("path")
    def className = column[String]("class_name")
    def namespace = column[String]("namespace")
    def parameters = column[JsMap]("parameters")
    def externalId = column[Option[String]]("external_id")
    def function = column[String]("function")
    def action = column[Action]("action")
    def source = column[JobDetails.Source]("source")
    def jobId = column[String]("job_id", O.PrimaryKey)
    def startTime = column[Option[Long]]("start_time")
    def endTime = column[Option[Long]]("end_time")
    def jobResult = column[Option[Either[String, JsData]]]("job_result")
    def status = column[JobDetails.Status]("status")
    def workerId = column[Option[String]]("worker_id")
    def createTime = column[Long]("create_time")

    override def * : ProvenShape[JobDetails] = {
      val shapedValue = (
        path,
        className,
        namespace,
        parameters,
        externalId,
        function,
        action,
        source,
        jobId,
        startTime,
        endTime,
        jobResult,
        status,
        workerId,
        createTime
        ).shaped
      shapedValue.<>({
        tuple => JobDetails(
          function = tuple._6,
          jobId = tuple._9,
          params = JobParams(tuple._1, tuple._2, tuple._4, tuple._7),
          context = tuple._3,
          externalId = tuple._5,
          source = tuple._8,
          startTime = tuple._10,
          endTime = tuple._11,
          jobResult = tuple._12,
          status = tuple._13,
          workerId = tuple._14,
          createTime = tuple._15
        )
      }, {
        (j: JobDetails) => Some(
          (j.params.filePath, j.params.className, j.context, j.params.arguments, j.externalId,
            j.function, j.params.action, j.source, j.jobId, j.startTime, j.endTime, j.jobResult, j.status, j.workerId, j.createTime)
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

  override def update(jobDetails: JobDetails): Future[Unit] = {
    run(table.insertOrUpdate(jobDetails)).map(_ => ())
  }

  override def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    run(table.filter(_.status inSetBind statuses).result)
  }

  override def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    val filtered = if (statuses.nonEmpty) table.filter(_.status inSet statuses) else table
    val query = filtered.sortBy(_.createTime.desc).drop(offset).take(limit)
    run(query.result)
  }

  override def clear(): Future[Unit] = run(table.delete).map(_ => ())

  type Table = JobDetailsTable
  type Elem =  Table#TableElementType

  type TQuery = Query[Table, Elem, Seq]

  override def getJobs(req: JobDetailsRequest): Future[JobDetailsResponse] = {
    def applyFilter(q: TQuery, f: FilterClause): TQuery = f match {
      case FilterClause.ByFunctionId(id) => q.filter(_.function === id)
      case FilterClause.ByWorkerId(id) => q.filter(_.workerId === id)
      case FilterClause.ByStatuses(s) if s.nonEmpty => q.filter(_.status inSet s)
      case FilterClause.ByStatuses(_) => q
    }

    val filtered = req.filters.foldLeft[TQuery](table)({case (q, f) => applyFilter(q, f)})
    val sorted = filtered.sortBy(_.createTime.desc)

    val q = sorted.drop(req.offset).take(req.limit)

    val countAction = sorted.length.result
    val action = countAction.flatMap(total => q.result.map(elems => JobDetailsResponse(elems, total)))

    run(action)
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
