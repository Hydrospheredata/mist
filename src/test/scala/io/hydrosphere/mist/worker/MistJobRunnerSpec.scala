package io.hydrosphere.mist.worker

import java.io.File
import java.nio.file.{Files, Paths}

import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.Flow
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.Action.Execute
import io.hydrosphere.mist.master.TestUtils.{AwaitSyntax, MockHttpServer}
import io.hydrosphere.mist.worker.runners.{JobRunner, MistJobRunner}
import io.hydrosphere.mist.worker.runners.python.PythonRunner
import io.hydrosphere.mist.worker.runners.scala.ScalaRunner
import org.apache.commons.io.FileUtils
import org.scalatest._
import org.mockito.Mockito.{doNothing, doReturn, never, spy, times, verify}

import scala.util.{Failure, Try}

class MistJobRunnerSpec extends FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  class StubContext extends NamedContext(null, "test")

  val basePath = "./target/data/jobs"
  val scalaJobPath = Paths.get(basePath, "test.jar").toString
  val pythonJobPath = Paths.get(basePath, "yo.py").toString
  val unknownJob = Paths.get(basePath, "yo.unknown").toString

  override protected def beforeAll(): Unit = {
    val f = Paths.get(basePath).toFile
    FileUtils.deleteDirectory(f)
    FileUtils.touch(new File(scalaJobPath))
    FileUtils.touch(new File(pythonJobPath))
    FileUtils.touch(new File(unknownJob))
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(basePath))
  }

  it("should select correct job runner by path ending") {
    val s = MistJobRunner.ExtensionMatchingRunnerSelector
    val r1 = s(new File(scalaJobPath))
    val r2 = s(new File(pythonJobPath))

    r1 shouldBe a [ScalaRunner]
    r2 shouldBe a [PythonRunner]
  }

  it("should throw exception when unknown file extension passed") {
    val s = MistJobRunner.ExtensionMatchingRunnerSelector
    intercept[IllegalArgumentException] {
      s(new File(unknownJob))
    }
  }

  it("should run job on selected JobRunner without downloading the file") {
    val mockRunner = mock[JobRunner]
    val myMockSelector: PartialFunction[File, JobRunner] = {
      case _: File => mockRunner
    }

    val mistJobRunner = MistJobRunner.create("localhost", 2004, myMockSelector, basePath)
    val spiedJobRunner = spy(mistJobRunner)

    when(mockRunner.run(any[RunJobRequest], any[NamedContext]))
      .thenReturn(Right[String, Map[String, Any]](Map.empty))

    val res = spiedJobRunner.run(
      RunJobRequest("test", JobParams(scalaJobPath, "SimpleContext$", Map.empty, Execute)),
      new StubContext
    )

    res.isRight shouldBe true

    verify(spiedJobRunner, never()).loadFromMaster(any[String])
  }

  it("should download from master file not resolved locally") {
    val routes = Flow[HttpRequest].map { request =>
      val uri = request.uri.toString()
      if (uri.endsWith(".jar")) {
        HttpResponse(status = StatusCodes.OK, entity = "JAR CONTENT")
      } else {
        HttpResponse(status = StatusCodes.NotFound)
      }
    }

    val future = MockHttpServer.onServer(routes, binding => {
      val mockRunner = mock[JobRunner]
      val myMockSelector: File => JobRunner = { _: File => mockRunner }
      val mistJobRunner = MistJobRunner.create("localhost", binding.localAddress.getPort, myMockSelector, basePath)
      mistJobRunner.loadFromMaster("test.jar")
    })
    val res = future.await
    res.isSuccess shouldBe true
    val file = res.get
    file.exists() shouldBe true
    file.getName shouldBe "test.jar"
  }

  it("should fail when downloading file failed") {
    val routes = Flow[HttpRequest].map { request =>
      HttpResponse(StatusCodes.InternalServerError)
    }

    val future = MockHttpServer.onServer(routes, binding => {
      val mockRunner = mock[JobRunner]
      val myMockSelector: File => JobRunner = { _: File => mockRunner }
      val mistJobRunner = MistJobRunner.create("localhost", binding.localAddress.getPort, myMockSelector, basePath)
      mistJobRunner.loadFromMaster("test.jar")
    })
    val res = future.await
    res.isFailure shouldBe true
  }


  it("should return left when error happen with file downloading") {
    val mistJobRunner = MistJobRunner("localhost", 2004, basePath)
    val spied = spy(mistJobRunner)

    doReturn(Failure(new Exception("error")))
      .when(spied)
      .loadFromMaster(any[String])

    val res = spied.run(
      RunJobRequest("test", JobParams(s"$basePath/not_exists.jar", "SimpleContext$", Map.empty, Execute)),
      new StubContext
    )

    res.isLeft shouldBe true
  }

}
