package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages
import io.hydrosphere.mist.Messages.StatusMessages.{FailedEvent, Register}
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.models._

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  *  Jobs starting/stopping, statuses, worker utility methods
  */
class JobService(workerManager: ActorRef, statusService: ActorRef) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[Seq[JobDetails]] =
    askStatus[Seq[JobDetails]](StatusMessages.RunningJobs)

  def markJobFailed(id: String, reason: String): Future[Unit] = {
    val event = FailedEvent(id, System.currentTimeMillis(), reason)
    statusService.ask(event).map(_ => ())
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] =
    askStatus[Option[JobDetails]](StatusMessages.GetById(id))

  def endpointHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    askStatus[Seq[JobDetails]](StatusMessages.GetEndpointHistory(id, limit, offset, statuses))

  def getHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    askStatus[Seq[JobDetails]](StatusMessages.GetHistory(limit, offset, statuses))

  def workers(): Future[Seq[WorkerLink]] =
    askManager[Seq[WorkerLink]](GetWorkers)

  def stopAllWorkers(): Future[Unit] = workerManager.ask(StopAllWorkers).map(_ => ())

  def stopWorker(workerId: String): Future[Unit] = workerManager.ask(StopWorker(workerId)).map(_ => ())

  def startJob(req: JobStartRequest): Future[ExecutionInfo] = {
    import req._

    val internalRequest = RunJobRequest(
      id = id,
      JobParams(
        filePath = endpoint.path,
        className = endpoint.className,
        arguments = parameters,
        action = action
      )
    )

    val startCmd = RunJobCommand(context, runMode, internalRequest)

    val registrationCommand = Register(
      request = internalRequest,
      endpoint = endpoint.name,
      context = context.name,
      source = source,
      externalId = externalId,
      workerId = startCmd.computeWorkerId()
    )

    for {
      _ <- statusService.ask(registrationCommand).mapTo[Unit]
      info <- workerManager.ask(startCmd).mapTo[ExecutionInfo]
    } yield info
  }

  def stopJob(jobId: String): Future[Option[JobDetails]] = {
    import cats.data._
    import cats.implicits._

    def tryCancel(d: JobDetails): Future[Unit] = {
      if (d.isCancellable ) {
        val f = workerManager ? CancelJobCommand(d.workerId, CancelJobRequest(jobId))
        f.map(_ => ())
      } else Future.successful(())
    }

    val out = for {
      details <- OptionT(jobStatusById(jobId))
      _ <- OptionT.liftF(tryCancel(details))
      // we should do second request to store,
      // because there is correct situation that job can be completed before cancelling
      updated <- OptionT(jobStatusById(jobId))
    } yield updated
    out.value
  }

  private def askManager[T: ClassTag](msg: Any): Future[T] = typedAsk[T](workerManager, msg)
  private def askStatus[T: ClassTag](msg: Any): Future[T] = typedAsk[T](statusService, msg)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] = ref.ask(msg).mapTo[T]

}

