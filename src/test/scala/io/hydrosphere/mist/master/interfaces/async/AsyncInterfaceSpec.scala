package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{RunMode, DevJobStartRequest, EndpointStartRequest}
import org.scalatest.FunSpec
import org.mockito.Mockito._
import org.mockito.Matchers._

class AsyncInterfaceSpec extends FunSpec {

  it("should run job on endpoint") {
    val master = mock(classOf[MasterService])

    val input = new TestAsyncInput

    val interface = new AsyncInterface(master, input, Source.Async("Test"))

    interface.start()

    val message =
      s"""
         |{
         |  "endpointId": "my-job",
         |  "parameters": { "x": "y" }
         |}
       """.stripMargin

    input.putMessage(message)
    verify(master).runJob(any[EndpointStartRequest], any[Source])
  }

  it("should run dev job") {
    val master = mock(classOf[MasterService])

    val input = new TestAsyncInput

    val interface = new AsyncInterface(master, input, Source.Async("Test"))

    interface.start()

    val message =
      s"""
         |{
         |  "fakeName": "my-job",
         |  "path": "path",
         |  "className": "MyJob",
         |  "parameters": { "x": "y" },
         |  "context": "foo",
         |  "externalId": "xxx"
         |}
       """.stripMargin

    input.putMessage(message)

    val expected = DevJobStartRequest(
      fakeName = "my-job",
      path = "path",
      className = "MyJob",
      parameters = Map("x" -> "y"),
      context = "foo",
      externalId = Some("xxx"),
      workerId = Some("test_id")
    )
    verify(master).devRun(expected, Source.Async("Test"))
  }


  class TestAsyncInput extends AsyncInput {

    private var handler: String => Unit = _

    def putMessage(s: String): Unit = handler(s)

    override def start(f: (String) => Unit): Unit =
      handler = f

    override def close(): Unit = {}
  }
}
