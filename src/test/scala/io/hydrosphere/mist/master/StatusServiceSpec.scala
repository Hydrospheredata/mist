package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import io.hydrosphere.mist.Messages.StatusMessages.{Register, UpdateStatus}
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobExecutionParams}
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.store.JobRepository
import org.scalatest.{FunSpecLike, Matchers}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

class StatusServiceSpec extends TestKit(ActorSystem("testFront"))
  with FunSpecLike
  with Matchers
  with Eventually {

  val jobExecutionParams = JobExecutionParams(
    path = "path",
    className = "MyClass",
    namespace = "namespace",
    parameters = Map("1" -> 2),
    externalId = Some("externalId"),
    route = Some("route"),
    action = Action.Execute
  )

  it("should register jobs") {
    val store = mock(classOf[JobRepository])

    val status = system.actorOf(StatusService.props(store))

    status ! Register("id", jobExecutionParams, Source.Async(Provider.Kafka))

    eventually(timeout(Span(1, Seconds))) {
      verify(store).update(any[JobDetails])
    }
  }

  it("should update status") {
    val store = mock(classOf[JobRepository])
    when(store.get(any[String])).thenReturn({
      val jobDetails = JobDetails(
        configuration = jobExecutionParams,
        jobId = "id",
        source = Source.Http
      )
      Some(jobDetails)
    })

    val status = system.actorOf(StatusService.props(store))

    status ! UpdateStatus("id", Status.Running, System.currentTimeMillis())

    eventually(timeout(Span(1, Seconds))) {
      verify(store).update(any[JobDetails])
    }
  }

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))
}
