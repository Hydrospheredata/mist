package io.hydrosphere.mist.worker

import java.io.File
import java.nio.file.Paths

import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.Action.Execute
import io.hydrosphere.mist.worker.runners.{JobRunner, MistJobRunner}
import io.hydrosphere.mist.worker.runners.python.PythonRunner
import io.hydrosphere.mist.worker.runners.scala.ScalaRunner
import org.apache.commons.io.FileUtils
import org.scalatest._
import org.mockito.Mockito.{spy, times, never, doReturn, verify, doNothing}
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
    FileUtils.touch(new File(scalaJobPath))
    FileUtils.touch(new File(pythonJobPath))
    FileUtils.touch(new File(unknownJob))
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteQuietly(new File(scalaJobPath))
    FileUtils.deleteQuietly(new File(pythonJobPath))
    FileUtils.deleteQuietly(new File(unknownJob))
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

    val mistJobRunner = MistJobRunner.create("localhost", 2004, myMockSelector)
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
    //TODO: add test mock http call (look for mvn or hdfs resolver test)
    1 shouldBe 2
  }

  it("should return left when error happen with file downloading") {
    val mistJobRunner = MistJobRunner("localhost", 2004)
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
