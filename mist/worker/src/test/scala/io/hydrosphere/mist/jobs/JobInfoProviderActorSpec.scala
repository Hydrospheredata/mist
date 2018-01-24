package io.hydrosphere.mist.jobs

import java.io.File
import java.nio.file.Paths

import akka.actor.{Actor, ActorSystem, Status}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import io.hydrosphere.mist.job.{Cache, JobInfo, JobInfoExtractor, JobInfoProviderActor}
import mist.api.args.{ArgInfo, UserInputArgument}
import mist.api.FullFnContext
import mist.api.args.MInt
import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.internal.BaseJobInstance
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import org.mockito.Mockito.{times, verify}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

class JobInfoProviderActorSpec extends TestKit(ActorSystem("WorkerSpec"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll {

  val baseDir = "./target/test-jobs"
  val jobPath = Paths.get(baseDir, "testJob.jar").toString

  override def beforeAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
    FileUtils.forceMkdir(f)
    FileUtils.writeStringToFile(new File(jobPath), "JAR CONTENT")
  }

  override def afterAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
  }

  describe("Actor") {

    it("should get job info when file is found") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
      testProbe.send(jobInfoProvider, GetJobInfo("Test", jobPath, "test"))
      testProbe.expectMsg(JobInfoData(
        lang = "scala",
        execute = Seq(UserInputArgument("test", MInt)),
        className = "Test",
        name="test"
      ))
    }

    it("should fail get job info when file not found") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      val testProbe = TestProbe()
      val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      val notExistingFile = "not_existing_file.jar"
      testProbe.send(jobInfoProvider, GetJobInfo("Test", notExistingFile, "test"))

      testProbe.expectMsgType[Status.Failure]
    }

    it("should handle failure of extraction when get job info") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Failure(new IllegalArgumentException("invalid job")))

      val testProbe = TestProbe()
      val jobInfoProvider = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProvider, GetJobInfo("Test", jobPath, "test"))

      testProbe.expectMsgType[Status.Failure]
    }

    it("should return validation success when validation is passed") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          TestJobInstance(Right(Map.empty)),
          JobInfoData()
        )))

      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProviderActor, ValidateJobParameters(
        "Test", jobPath, "test", Map.empty))
      testProbe.expectMsgType[Status.Success]

    }
    it("should return failure when file not found") {
      val jobInfoExtractor = mock[JobInfoExtractor]

      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProviderActor, ValidateJobParameters(
        "Test", "not_existing_file.jar", "test", Map.empty))

      testProbe.expectMsgType[Status.Failure]

    }
    it("should return failure when fail to extract instance") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Failure(new IllegalArgumentException("invalid")))

      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProviderActor, ValidateJobParameters(
        "Test", jobPath, "test", Map.empty))
      testProbe.expectMsgType[Status.Failure]
    }
    it("should return failure when validation fails") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          TestJobInstance(Right(Map.empty)),
          JobInfoData()
        )))
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          TestJobInstance(Left(new IllegalArgumentException("invalid"))),
          JobInfoData()
        )))

      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProviderActor, ValidateJobParameters(
        "Test", jobPath, "test", Map.empty))
      testProbe.expectMsgType[Status.Failure]
    }

    it("should cache job info when requested") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))

      testProbe.send(jobInfoProviderActor, GetJobInfo("Test", jobPath, "test"))
      testProbe.expectMsgType[JobInfoData]
      testProbe.send(jobInfoProviderActor, GetCacheSize)
      testProbe.expectMsg(1)
    }

    it("should hit cache when get job info") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
      // heat up cache
      testProbe.send(jobInfoProviderActor, GetJobInfo("Test", jobPath, "test"))
      testProbe.expectMsgType[JobInfoData]
      testProbe.send(jobInfoProviderActor, GetJobInfo("Test", jobPath, "test"))
      verify(jobInfoExtractor, times(1)).extractInfo(any[File], any[String])
    }

    it("should hit cache when validate job info") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          instance = TestJobInstance(Right(Map.empty)),
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
      // heat up cache
      testProbe.send(jobInfoProviderActor, GetJobInfo("Test", jobPath, "test"))
      testProbe.expectMsgType[JobInfoData]
      testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", jobPath, "test", Map.empty))
      testProbe.expectMsgType[Status.Success]

      verify(jobInfoExtractor, times(1)).extractInfo(any[File], any[String])
    }

    it("should cache job info when validate job") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          instance = TestJobInstance(Right(Map.empty)),
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
      // heat up cache
      testProbe.send(jobInfoProviderActor, ValidateJobParameters("Test", jobPath, "test", Map.empty))
      testProbe.expectMsgType[Status.Success]
      testProbe.send(jobInfoProviderActor, GetCacheSize)
      testProbe.expectMsg(1)
    }

    it("should get all items in request") {
      val jobInfoExtractor = mock[JobInfoExtractor]
      when(jobInfoExtractor.extractInfo(any[File], any[String]))
        .thenReturn(Success(JobInfo(
          instance = TestJobInstance(Right(Map.empty)),
          data = JobInfoData(
            lang = "scala",
            execute = Seq(UserInputArgument("test", MInt)),
            className = "Test"
          )
        )))
      val testProbe = TestProbe()
      val jobInfoProviderActor = TestActorRef[Actor](JobInfoProviderActor.props(jobInfoExtractor))
      // heat up cache
      testProbe.send(jobInfoProviderActor,
        GetAllJobInfo(List(GetJobInfo("Test", jobPath, "test"))))
      testProbe.expectMsg(Seq(JobInfoData(
        "test",
        "scala",
        Seq(UserInputArgument("test", MInt)),
        className="Test"
      )))
    }
  }
  describe("Cache") {
    it("should put item and create new cache with") {
      val cache = Cache(300 seconds, Map.empty[String, (Long, Int)])
      val updated = cache.put("test", 42)
      cache.size shouldBe 0
      updated.size shouldBe 1
    }
    it("should filter expired item") {
      val cache: Cache[String, Int] = Cache(0 seconds)
      val updated = cache.put("test", 42)
      val item = updated.get("test")
      item should not be defined
    }

    it("should remove expired elements on evict") {
      val cache: Cache[String, Int] = Cache(0 seconds)
      val updated = cache.put("test", 42)
      updated.size shouldBe 1
      val evicted = updated.evictAll
      evicted.size shouldBe 0
    }
  }

  def TestJobInstance(validationResult: Either[Throwable, Map[String, Any]]): BaseJobInstance = new BaseJobInstance {
    override def run(jobCtx: FullFnContext): Either[Throwable, JsLikeData] = Right(JsLikeNull)

    override def validateParams(params: Map[String, Any]): Either[Throwable, Any] = validationResult

    override def describe(): Seq[ArgInfo] = Seq.empty
  }
}
