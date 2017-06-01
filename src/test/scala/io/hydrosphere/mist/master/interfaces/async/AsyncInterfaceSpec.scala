package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.JobStartRequest
import org.scalatest.FunSpec
import org.mockito.Mockito._
import org.mockito.Matchers._

class AsyncInterfaceSpec extends FunSpec {

  it("should run job") {
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
    verify(master).runJob(any[JobStartRequest], any[Source], any[Action])
  }


  class TestAsyncInput extends AsyncInput {

    private var handler: String => Unit = _

    def putMessage(s: String): Unit = handler(s)

    override def start(f: (String) => Unit): Unit =
      handler = f

    override def close(): Unit = {}
  }
}
