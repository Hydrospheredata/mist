package io.hydrosphere.mist.master.interfaces.async

import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.MainService
import io.hydrosphere.mist.master.models.{DevJobStartRequest, FunctionStartRequest}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSpec
import org.mockito.Mockito._
import org.mockito.Matchers._
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._
import mist.api.data._


class AsyncInterfaceSpec extends FunSpec {

  it("should run job on function") {
    val master = mock(classOf[MainService])

    val input = new TestAsyncInput

    val interface = new AsyncInterface(master, input, "Test")

    interface.start()

    val message =
      s"""
         |{
         |  "functionId": "my-job",
         |  "parameters": { "x": "y" }
         |}
       """.stripMargin

    input.putMessage(message)
    verify(master).runJob(any[FunctionStartRequest], any[Source])
  }

  it("should run dev job") {
    val master = mock(classOf[MainService])

    val input = new TestAsyncInput

    val interface = new AsyncInterface(master, input, "Test")

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
      parameters = JsMap("x" -> "y".js),
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
