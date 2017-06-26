package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorSelection
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.Messages.JobMessages.{CancelJobRequest, JobIsCancelled}
import io.hydrosphere.mist.Messages.ListRoutes
import io.hydrosphere.mist.Messages.StatusMessages.RunningJobs
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.jobs.{JobDefinition, JobDetails}
import io.hydrosphere.mist.master.WorkerLink
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.concurrent.duration._

sealed trait Command
case object Exit extends Command

trait RemoteCliCommand[Resp] extends Command {

  val request: Any

  val headers: List[String]

  implicit val timeout = Timeout(10.second)

  def exec(ref: ActorSelection): Unit = {
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


object RunningJobsCmd extends RemoteCliCommand[List[JobDetails]] {

  override val request = RunningJobs
  override val headers = List("UID", "START TIME", "NAMESPACE", "EXT ID", "ROUTE", "SOURCE", "STATUS")

  override def convert(resp: List[JobDetails]): List[Row] = {
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

object ListWorkersCmd extends RemoteCliCommand[List[WorkerLink]] {

  override val request = GetWorkers
  override val headers = List("ID", "ADDRESS")

  override def convert(resp: List[WorkerLink]): Seq[Row] =
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

object ListRoutesCmd extends RemoteCliCommand[Seq[JobDefinition]] {

  override val request = ListRoutes

  override def convert(resp: Seq[JobDefinition]): Seq[Row] =
    resp.map(d => Row.create(d.name, d.nameSpace, d.path, d.className))

  override val headers: List[String] = List("ROUTE", "NAMESPACE", "PATH", "CLASS NAME")
}

object Command {

  def parse(input: String): Option[Command] = input match {
    case msg if msg.contains(Constants.CLI.Commands.listJobs) =>
      Some(RunningJobsCmd)

    case msg if msg.contains(Constants.CLI.Commands.listWorkers) =>
      Some(ListWorkersCmd)

    case msg if msg.contains(Constants.CLI.Commands.stopWorker) =>
      val name = msg.substring(Constants.CLI.Commands.stopWorker.length).trim
      Some(new StopWorkerCmd(name))

    case msg if msg.contains(Constants.CLI.Commands.stopJob) =>
      val params = msg.substring(Constants.CLI.Commands.stopJob.length).trim.split(' ')
      Some(new StopJobCmd(params(0), params(1)))

    case msg if msg.contains(Constants.CLI.Commands.stopAllWorkers) =>
      Some(StopAllWorkersCmd)

    case msg if msg.contains(Constants.CLI.Commands.listRouters) =>
      Some(ListRoutesCmd)

    case Constants.CLI.Commands.exit =>
      Some(Exit)

    case _ => None
  }
}

