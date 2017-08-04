package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.Messages.JobMessages.{CancelJobRequest, JobIsCancelled}
import io.hydrosphere.mist.Messages.{ListRoutes, RunJobCli}
import io.hydrosphere.mist.Messages.StatusMessages.RunningJobs
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.jobs.{JobDetails, JobInfo}
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{FullEndpointInfo, JobStartResponse}
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.concurrent.duration._

sealed trait Command
case object Exit extends Command
case object Help extends Command
case object Empty extends Command

trait RemoteCliCommand[Resp] extends Command {

  val request: Any

  val headers: List[String]

  implicit val timeout = Timeout(10.second)

  def exec(ref: ActorRef): Unit = {
    val future = ref ? request
    val r1 = Await.result(future, 10.second)
    val result = r1.asInstanceOf[Resp]
    val rows = convert(result)

    val table = ConsoleTable(headers, rows)

    print(table.prettyPrint)
  }

  def convert(resp: Resp): Seq[Row]
}

trait RemoteUnitCliCommand extends RemoteCliCommand[Unit] {

  override val headers = List("RESULT")

  override def convert(resp: Unit): Seq[Row] =
    List(Row.create("Ok"))
}


object RunningJobsCmd extends RemoteCliCommand[Seq[JobDetails]] {

  override val request = RunningJobs
  override val headers = List("UID", "START TIME", "NAMESPACE", "EXT ID", "ROUTE", "SOURCE", "STATUS")

  override def convert(resp: Seq[JobDetails]): Seq[Row] = {
    def toTimeSting(i: Long) = new DateTime(i).toString
    resp.map(s => {
      Row.create(
        s.jobId,
        s.startTime.map(toTimeSting).getOrElse(""),
        s.context,
        s.externalId.getOrElse(""),
        s.endpoint,
        s.source.toString,
        s.status.toString
      )
    })
  }

}

object ListWorkersCmd extends RemoteCliCommand[Seq[WorkerLink]] {

  override val request = GetWorkers
  override val headers = List("ID", "ADDRESS")

  override def convert(resp: Seq[WorkerLink]): Seq[Row] =
    resp.map(s => Row.create(s.name, s.address))

}

case class StopWorkerCmd(name: String) extends RemoteUnitCliCommand {

  override val request = StopWorker(name)
}

case class StopJobCmd(namespace: String, id: String) extends RemoteCliCommand[JobIsCancelled] {

  override val request = CancelJobCommand(namespace, CancelJobRequest(id))

  override val headers = List("ID", "TIME")

  override def convert(resp: JobIsCancelled): Seq[Row] =
    List(Row.create(
      resp.id,
      new DateTime(resp.time).toString
    ))
}

object StopAllWorkersCmd extends RemoteUnitCliCommand {
  override val request = StopAllWorkers
}

object ListRoutesCmd extends RemoteCliCommand[Seq[FullEndpointInfo]] {

  override val request = ListRoutes

  override def convert(resp: Seq[FullEndpointInfo]): Seq[Row] =
    resp.map(i => Row.create(i.config.name, i.config.defaultContext, i.config.path, i.config.className))

  override val headers: List[String] = List("ROUTE", "DEFAULT CONTEXT", "PATH", "CLASS NAME")
}

case class StartJobCmd(
  endpoint: String,
  extId: Option[String],
  params: Map[String, Any]
) extends RemoteCliCommand[JobStartResponse] {

  override val request = RunJobCli(endpoint, extId, params)

  override def convert(resp: JobStartResponse): Seq[Row] = {
    Seq(Row.create(resp.id))
  }

  override val headers: List[String] = List("ID")
}

object Command {

  import Constants.CLI.Commands

  val startR = "([a-zA-Z0-9-_]+)\\s([a-zA-Z0-9-_]+\\s)?('.+')?".r

  def parse(input: String): Either[String, Command] = input match {
    case msg if msg.startsWith(Commands.listJobs) =>
      Right(RunningJobsCmd)

    case msg if msg.startsWith(Commands.listWorkers) =>
      Right(ListWorkersCmd)

    case msg if msg.startsWith(Commands.stopWorker) =>
      val name = msg.substring(Commands.stopWorker.length).trim
      Right(StopWorkerCmd(name))

    case msg if msg.startsWith(Commands.stopJob) =>
      val params = msg.substring(Commands.stopJob.length).trim.split(' ')
      Right(StopJobCmd(params(0), params(1)))

    case msg if msg.startsWith(Commands.stopAllWorkers) =>
      Right(StopAllWorkersCmd)

    case msg if msg.startsWith(Commands.startJob) =>
      val params = msg.substring(Commands.startJob.length)
      parseStartCommand(params)

    case msg if msg.contains(Commands.listRouters) =>
      Right(ListRoutesCmd)

    case Commands.exit =>
      Right(Exit)

    case Commands.help =>
      Right(Help)

    case _ => Right(Empty)
  }

  private def parseStartCommand(s: String): Either[String, StartJobCmd] = {
    import cats.implicits._
    import spray.json._
    import JsonCodecs._

    def parseArgs(args: String): Either[String, Map[String, Any]] = {
      if (args.length == 0)
        Right(Map.empty)
      else {
        Either.catchNonFatal {
          args.parseJson.convertTo[Map[String, Any]]
        }.leftMap(e => e.toString)
      }
    }


    startR.findFirstMatchIn(s) match {
      case Some(matched) => matched.subgroups match {
        case id :: Nil => Right(StartJobCmd(id, None, Map.empty))

        case id :: extId :: data :: Nil =>
          if (data == null) {
            Right(StartJobCmd(id, Option(extId), Map.empty))
          } else {
            val unquoted = data.substring(1, data.length - 1)
            parseArgs(unquoted).map(args => StartJobCmd(id, Option(extId), args))
          }

        case x =>
          Left(s"Can not parse start command with args $s")
      }
      case None =>
        Left(s"Can not parse start command with args $s")
     }
  }
}

