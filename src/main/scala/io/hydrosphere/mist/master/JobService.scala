package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import cats.data._
import cats.implicits._
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
//TODO: refactor cli interface
class JobService(val workerManager: ActorRef, statusService: ActorRef) {

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

  def workerByJobId(jobId: String): Future[Option[WorkerLink]] = {
    val res = for {
      job        <- OptionT(jobStatusById(jobId))
      workerLink <- OptionT(fetchWorkerLink(job))
    } yield workerLink
    res.value
  }

  private def fetchWorkerLink(job: JobDetails): Future[Option[WorkerLink]] = {
    val res = for {
      startTime  <- OptionT.fromOption[Future](job.startTime)
      (state, _) <- OptionT(workerInitInfo(job.workerId))
      l          <- OptionT.fromOption[Future](toWorkerLink(state))
      link       <- OptionT.fromOption[Future](if (state.timestamp <= startTime) Some(l) else None)
      workerLink =  link.copy(name = job.workerId)
    } yield workerLink
    res.value
  }

  private def toWorkerLink(state: WorkerState): Option[WorkerLink] = state match {
    case Started(_, addr, _, sparkUi, _) => Some(WorkerLink("", addr.toString, sparkUi))
    case _                            => None
  }

  def getWorkerInfo(workerId: String): Future[Option[WorkerFullInfo]] = {
    val res = for {
      workerInfo                       <- OptionT(workerInitInfo(workerId))
      jobs                             <- OptionT.liftF(jobsByWorker(workerId, workerInfo._1))
      jobsLink                         =  jobs.map(toJobLinks)
      (initInfo, address, sparkUi)     =  workerInfo match {
                                            case (Started(_, a, _, s, _), i) => (i, a.toString, s)
                                            case (_, i) => (i, "", None)
                                          }
    } yield WorkerFullInfo(workerId, address, sparkUi, jobsLink, initInfo)

    res.value
  }

  private def workerInitInfo(workerId: String): Future[Option[(WorkerState, WorkerInitInfo)]] =
    askManager[Option[(WorkerState, WorkerInitInfo)]](GetInitInfo(workerId))

  private def jobsByWorker(
    workerId: String,
    state: WorkerState
  ): Future[Seq[JobDetails]] =
    askStatus[Seq[JobDetails]](StatusMessages.RunningJobsByWorker(workerId, state))

  private def toJobLinks(job: JobDetails): JobDetailsLink = JobDetailsLink(
      job.jobId, job.source, job.startTime, job.endTime,
      job.status, job.endpoint, job.workerId, job.createTime
  )

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

