package io.hydrosphere.mist.jobs.store

import java.io.File

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.jobs.{JobConfiguration, JobDetails}
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.utils.Logger
import io.getquill._
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.TypeAlias.{JobParameters, JobResponseOrError}
import io.hydrosphere.mist.utils.json.JobDetailsJsonSerialization
import org.flywaydb.core.Flyway
import spray.json.{pimpAny, pimpString}

import scala.collection.immutable.Seq

object SqliteJobRepository extends JobRepository with JobDetailsJsonSerialization with Logger {
  
  lazy private val ctx = {
    val jdbcPath = s"jdbc:sqlite:${MistConfig.History.filePath}"
    logger.info(getClass.getResource("/db/migrations").getPath)
    val dir = new File(getClass.getResource("/db").toExternalForm)
    logger.info(dir.isDirectory.toString)
    logger.info(dir.exists().toString)
    logger.info(dir.canRead.toString)
    logger.info(dir.toString)
    val flyway = new Flyway()
    flyway.setLocations("/db/migrations")
    flyway.setDataSource(jdbcPath, null, null)
    flyway.migrate()
    
    val config = 
      s"""
         | driverClassName=org.sqlite.JDBC
         | jdbcUrl="$jdbcPath"
     """.stripMargin
    new SqliteJdbcContext[SnakeCase](ConfigFactory.parseString(config))
  }
  
  private implicit val encodeSource = MappedEncoding[JobDetails.Source, String](_.toString)
  private implicit val decodeSource = MappedEncoding[String, JobDetails.Source](JobDetails.Source(_))
  
  private implicit val encodeJobResult = MappedEncoding[JobResponseOrError, String](_.toJson.compactPrint)
  private implicit val decodeJobResult = MappedEncoding[String, JobResponseOrError](_.parseJson.convertTo[JobResponseOrError])
  
  private implicit val encodeStatus = MappedEncoding[JobDetails.Status, String](_.toString)
  private implicit val decodeStatus = MappedEncoding[String, JobDetails.Status](JobDetails.Status(_))
  
  private implicit val encodeParameters = MappedEncoding[JobParameters, String](_.toJson.compactPrint)
  private implicit val decodeParameters = MappedEncoding[String, JobParameters](_.parseJson.convertTo[JobParameters])
  
  private implicit val encodeAction = MappedEncoding[JobConfiguration.Action, String](_.toString)
  private implicit val decodeAction = MappedEncoding[String, JobConfiguration.Action](JobConfiguration.Action(_))

  private def add(jobDetails: JobDetails): Unit = {
    import ctx._
    val q = quote {
      query[JobDetails].insert(
        _.configuration.path -> ctx.lift(jobDetails.configuration.path),
        _.configuration.className -> ctx.lift(jobDetails.configuration.className),
        _.configuration.namespace -> ctx.lift(jobDetails.configuration.namespace),
        _.configuration.parameters -> ctx.lift(jobDetails.configuration.parameters),
        _.configuration.externalId -> ctx.lift(jobDetails.configuration.externalId),
        _.configuration.route -> ctx.lift(jobDetails.configuration.route),
        _.configuration.action -> ctx.lift(jobDetails.configuration.action),
        _.source -> ctx.lift(jobDetails.source),
        _.jobId -> ctx.lift(jobDetails.jobId),
        _.startTime -> ctx.lift(jobDetails.startTime),
        _.endTime -> ctx.lift(jobDetails.endTime),
        _.jobResult -> ctx.lift(jobDetails.jobResult),
        _.status -> ctx.lift(jobDetails.status)
      )
    }
    run(q)
  }
  

  override def remove(jobId: String): Unit = {
    import ctx._
    val q = quote {
      query[JobDetails].filter(jobDetails => jobDetails.jobId == ctx.lift(jobId)).delete
    }
    ctx.run(q)
  }

  override def get(jobId: String): Option[JobDetails] = {
    import ctx._
    val q = quote {
      query[JobDetails].filter(jobDetails => jobDetails.jobId == ctx.lift(jobId))
    }
    val response: Seq[JobDetails] = ctx.run(q)
    response.headOption
  }

  override def size: Long = {
    import ctx._
    val q = quote {
      query[JobDetails]
    }
    ctx.run(q.size)
  }

  override def clear(): Unit = {
    import ctx._
    val q = quote {
      query[JobDetails].delete
    }
    ctx.run(q)
  }

  override def update(jobDetails: JobDetails): Unit = {
    get(jobDetails.jobId) match {
      case Some(_) =>
        import ctx._
        val q = quote {
          query[JobDetails].filter(j => j.jobId == ctx.lift(jobDetails.jobId)).update(
            _.configuration.path -> ctx.lift(jobDetails.configuration.path),
            _.configuration.className -> ctx.lift(jobDetails.configuration.className),
            _.configuration.namespace -> ctx.lift(jobDetails.configuration.namespace),
            _.configuration.parameters -> ctx.lift(jobDetails.configuration.parameters),
            _.configuration.externalId -> ctx.lift(jobDetails.configuration.externalId),
            _.configuration.route -> ctx.lift(jobDetails.configuration.route),
            _.configuration.action -> ctx.lift(jobDetails.configuration.action),
            _.source -> ctx.lift(jobDetails.source),
            _.jobId -> ctx.lift(jobDetails.jobId),
            _.startTime -> ctx.lift(jobDetails.startTime),
            _.endTime -> ctx.lift(jobDetails.endTime),
            _.jobResult -> ctx.lift(jobDetails.jobResult),
            _.status -> ctx.lift(jobDetails.status)
          )
        }
        ctx.run(q)
      case None => add(jobDetails)
    }
  }

  override def filteredByStatuses(statuses: List[Status]): List[JobDetails] = {
    import ctx._
    val q = quote {
      query[JobDetails].filter(j => liftQuery(statuses).contains(j.status))
    }
    ctx.run(q)
  }
}
